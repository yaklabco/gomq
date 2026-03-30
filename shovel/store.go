package shovel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// storeFilePerms is the file mode for the shovel and federation JSON files.
const storeFilePerms = 0o644

// storeDirPerms is the directory mode for the shovel data directory.
const storeDirPerms = 0o750

// Store manages shovels and federation links with JSON persistence.
type Store struct {
	mu             sync.RWMutex
	shovels        map[string]*Shovel
	shovelConfigs  map[string]*ShovelConfig
	fedLinks       map[string]*Shovel
	fedConfigs     map[string]*FederationConfig
	shovelPath     string
	federationPath string
	ctx            context.Context
	cancelAll      context.CancelFunc
}

// ShovelConfig holds the configuration for a shovel as submitted via
// the HTTP API and persisted to disk.
type ShovelConfig struct {
	Name       string `json:"name"`
	SrcURI     string `json:"src_uri"`
	SrcQueue   string `json:"src_queue"`
	DestURI    string `json:"dest_uri"`
	DestType   string `json:"dest_type"`   // "amqp" or "http"
	Exchange   string `json:"exchange"`    // for AMQP destination
	RoutingKey string `json:"routing_key"` // for AMQP destination
	HTTPMethod string `json:"http_method"` // for HTTP destination
	AckMode    string `json:"ack_mode"`    // "on-confirm", "on-publish", "no-ack"
	Prefetch   int    `json:"prefetch"`
	VHost      string `json:"vhost"`
}

// Validate checks that a shovel config has the required fields.
func (c *ShovelConfig) Validate() error {
	if c.Name == "" {
		return errors.New("shovel name is required")
	}
	if c.SrcURI == "" {
		return errors.New("source URI is required")
	}
	if c.SrcQueue == "" {
		return errors.New("source queue is required")
	}
	if c.DestURI == "" {
		return errors.New("destination URI is required")
	}
	return nil
}

// NewStore creates a store that persists shovel and federation
// configurations to the given data directory.
func NewStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, storeDirPerms); err != nil { //nolint:gosec // data directory, not world-writable
		return nil, fmt.Errorf("create shovel data dir: %w", err)
	}

	st := &Store{
		shovels:        make(map[string]*Shovel),
		shovelConfigs:  make(map[string]*ShovelConfig),
		fedLinks:       make(map[string]*Shovel),
		fedConfigs:     make(map[string]*FederationConfig),
		shovelPath:     filepath.Join(dataDir, "shovels.json"),
		federationPath: filepath.Join(dataDir, "federation.json"),
	}

	if err := st.loadShovels(); err != nil {
		return nil, fmt.Errorf("load shovels: %w", err)
	}
	if err := st.loadFederation(); err != nil {
		return nil, fmt.Errorf("load federation: %w", err)
	}

	return st, nil
}

// Add creates and starts a new shovel from the given configuration.
func (st *Store) Add(cfg *ShovelConfig) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate shovel config: %w", err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	if _, exists := st.shovelConfigs[cfg.Name]; exists {
		// Stop existing shovel before replacing.
		if existing, ok := st.shovels[cfg.Name]; ok {
			_ = existing.Stop() //nolint:errcheck // best-effort stop before replace
		}
	}

	shvl := buildShovel(cfg)
	st.shovels[cfg.Name] = shvl
	st.shovelConfigs[cfg.Name] = cfg

	if err := st.saveShovels(); err != nil {
		delete(st.shovels, cfg.Name)
		delete(st.shovelConfigs, cfg.Name)
		return fmt.Errorf("persist shovel: %w", err)
	}

	if st.ctx != nil {
		if err := shvl.Start(st.ctx); err != nil {
			return fmt.Errorf("start shovel %q: %w", cfg.Name, err)
		}
	}

	return nil
}

// Delete stops and removes a shovel by name.
func (st *Store) Delete(name string) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	shvl, ok := st.shovels[name]
	if !ok {
		return fmt.Errorf("shovel %q not found", name)
	}

	if err := shvl.Stop(); err != nil {
		return fmt.Errorf("stop shovel %q: %w", name, err)
	}

	delete(st.shovels, name)
	delete(st.shovelConfigs, name)

	if err := st.saveShovels(); err != nil {
		return fmt.Errorf("persist after delete: %w", err)
	}

	return nil
}

// List returns all shovel configurations.
func (st *Store) List() []*ShovelConfig {
	st.mu.RLock()
	defer st.mu.RUnlock()

	configs := make([]*ShovelConfig, 0, len(st.shovelConfigs))
	for _, cfg := range st.shovelConfigs {
		configs = append(configs, cfg)
	}
	return configs
}

// ShovelStatus returns the status of a named shovel.
func (st *Store) ShovelStatus(name string) string {
	st.mu.RLock()
	defer st.mu.RUnlock()

	shvl, ok := st.shovels[name]
	if !ok {
		return StatusStopped
	}
	return shvl.Status()
}

// AddFederationLink creates and starts a federation link.
func (st *Store) AddFederationLink(cfg *FederationConfig) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate federation config: %w", err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	if _, exists := st.fedConfigs[cfg.Name]; exists {
		if existing, ok := st.fedLinks[cfg.Name]; ok {
			_ = existing.Stop() //nolint:errcheck // best-effort stop before replace
		}
	}

	localURI := "amqp://guest:guest@127.0.0.1:5672/"
	shvl := NewFederationLinkFromConfig(cfg, localURI)
	st.fedLinks[cfg.Name] = shvl
	st.fedConfigs[cfg.Name] = cfg

	if err := st.saveFederation(); err != nil {
		delete(st.fedLinks, cfg.Name)
		delete(st.fedConfigs, cfg.Name)
		return fmt.Errorf("persist federation link: %w", err)
	}

	if st.ctx != nil {
		if err := shvl.Start(st.ctx); err != nil {
			return fmt.Errorf("start federation link %q: %w", cfg.Name, err)
		}
	}

	return nil
}

// DeleteFederationLink stops and removes a federation link by name.
func (st *Store) DeleteFederationLink(name string) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	link, ok := st.fedLinks[name]
	if !ok {
		return fmt.Errorf("federation link %q not found", name)
	}

	if err := link.Stop(); err != nil {
		return fmt.Errorf("stop federation link %q: %w", name, err)
	}

	delete(st.fedLinks, name)
	delete(st.fedConfigs, name)

	if err := st.saveFederation(); err != nil {
		return fmt.Errorf("persist after federation delete: %w", err)
	}

	return nil
}

// ListFederationLinks returns all federation link configurations.
func (st *Store) ListFederationLinks() []*FederationConfig {
	st.mu.RLock()
	defer st.mu.RUnlock()

	configs := make([]*FederationConfig, 0, len(st.fedConfigs))
	for _, cfg := range st.fedConfigs {
		configs = append(configs, cfg)
	}
	return configs
}

// FederationLinkStatuses returns status information for all links.
func (st *Store) FederationLinkStatuses() []*FederationLinkStatus {
	st.mu.RLock()
	defer st.mu.RUnlock()

	statuses := make([]*FederationLinkStatus, 0, len(st.fedConfigs))
	for _, cfg := range st.fedConfigs {
		shvl := st.fedLinks[cfg.Name]
		statuses = append(statuses, FederationStatus(cfg, shvl))
	}
	return statuses
}

// StartAll starts all shovels and federation links. Must be called
// with a context that controls their lifecycle.
func (st *Store) StartAll(ctx context.Context) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	sCtx, cancel := context.WithCancel(ctx)
	st.ctx = sCtx
	st.cancelAll = cancel

	var firstErr error
	for name, shvl := range st.shovels {
		if err := shvl.Start(sCtx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("start shovel %q: %w", name, err)
		}
	}
	for name, link := range st.fedLinks {
		if err := link.Start(sCtx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("start federation link %q: %w", name, err)
		}
	}

	return firstErr
}

// StopAll stops all shovels and federation links.
func (st *Store) StopAll() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.cancelAll != nil {
		st.cancelAll()
		st.cancelAll = nil
	}

	var firstErr error
	for name, shvl := range st.shovels {
		if err := shvl.Stop(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("stop shovel %q: %w", name, err)
		}
	}
	for name, link := range st.fedLinks {
		if err := link.Stop(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("stop federation link %q: %w", name, err)
		}
	}

	st.ctx = nil
	return firstErr
}

// --- Persistence ---

func (st *Store) saveShovels() error {
	return saveJSON(st.shovelPath, st.shovelConfigSlice())
}

func (st *Store) saveFederation() error {
	return saveJSON(st.federationPath, st.fedConfigSlice())
}

func (st *Store) loadShovels() error {
	configs, err := loadJSON[ShovelConfig](st.shovelPath)
	if err != nil {
		return err
	}
	for _, cfg := range configs {
		st.shovelConfigs[cfg.Name] = cfg
		st.shovels[cfg.Name] = buildShovel(cfg)
	}
	return nil
}

func (st *Store) loadFederation() error {
	configs, err := loadJSON[FederationConfig](st.federationPath)
	if err != nil {
		return err
	}
	for _, cfg := range configs {
		st.fedConfigs[cfg.Name] = cfg
		localURI := "amqp://guest:guest@127.0.0.1:5672/"
		st.fedLinks[cfg.Name] = NewFederationLinkFromConfig(cfg, localURI)
	}
	return nil
}

func (st *Store) shovelConfigSlice() []*ShovelConfig {
	configs := make([]*ShovelConfig, 0, len(st.shovelConfigs))
	for _, cfg := range st.shovelConfigs {
		configs = append(configs, cfg)
	}
	return configs
}

func (st *Store) fedConfigSlice() []*FederationConfig {
	configs := make([]*FederationConfig, 0, len(st.fedConfigs))
	for _, cfg := range st.fedConfigs {
		configs = append(configs, cfg)
	}
	return configs
}

// saveJSON atomically writes data as JSON to the given path.
func saveJSON(path string, data any) error {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, jsonData, storeFilePerms); err != nil { //nolint:gosec // config file
		return fmt.Errorf("write %s: %w", tmp, err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s to %s: %w", tmp, path, err)
	}

	return nil
}

// loadJSON reads a JSON array from the given file path. Returns an
// empty slice if the file does not exist.
func loadJSON[T any](path string) ([]*T, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	var items []*T
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	return items, nil
}

// buildShovel creates a Shovel from a ShovelConfig.
func buildShovel(cfg *ShovelConfig) *Shovel {
	src := NewAMQPSource(cfg.SrcURI, cfg.SrcQueue, cfg.Prefetch)

	var dst Destination
	switch cfg.DestType {
	case "http":
		dst = NewHTTPDestination(cfg.DestURI, cfg.HTTPMethod, nil)
	default: // "amqp" or empty
		dst = NewAMQPDestination(cfg.DestURI, cfg.Exchange, cfg.RoutingKey)
	}

	return NewShovel(cfg.Name, src, dst, AckMode(cfg.AckMode), cfg.Prefetch)
}
