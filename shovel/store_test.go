package shovel

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStoreAddDeleteList(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() error: %v", err)
	}

	cfg := &ShovelConfig{
		Name:     "test-shovel",
		SrcURI:   "amqp://source:5672",
		SrcQueue: "src-queue",
		DestURI:  "amqp://dest:5672",
		DestType: "amqp",
		Exchange: "dest-exchange",
		AckMode:  "on-publish",
		Prefetch: 100,
		VHost:    "/",
	}

	if err := store.Add(cfg); err != nil {
		t.Fatalf("Add() error: %v", err)
	}

	// List should return the config.
	configs := store.List()
	if len(configs) != 1 {
		t.Fatalf("List() returned %d configs, want 1", len(configs))
	}
	if configs[0].Name != "test-shovel" {
		t.Errorf("config name = %q, want %q", configs[0].Name, "test-shovel")
	}

	// Delete.
	if err := store.Delete("test-shovel"); err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	configs = store.List()
	if len(configs) != 0 {
		t.Errorf("List() after delete returned %d configs, want 0", len(configs))
	}
}

func TestStoreDeleteNotFound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() error: %v", err)
	}

	if err := store.Delete("nonexistent"); err == nil {
		t.Error("Delete() expected error for nonexistent shovel, got nil")
	}
}

func TestStorePersistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() error: %v", err)
	}

	cfg := &ShovelConfig{
		Name:     "persist-test",
		SrcURI:   "amqp://source:5672",
		SrcQueue: "queue1",
		DestURI:  "amqp://dest:5672",
		DestType: "amqp",
		Exchange: "ex1",
		VHost:    "/",
	}

	if err := store.Add(cfg); err != nil {
		t.Fatalf("Add() error: %v", err)
	}

	// Verify file was created.
	shovelFile := filepath.Join(dir, "shovels.json")
	if _, statErr := os.Stat(shovelFile); statErr != nil {
		t.Fatalf("shovels.json not created: %v", statErr)
	}

	// Reopen store - configs should be loaded.
	store2, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() reopen error: %v", err)
	}

	configs := store2.List()
	if len(configs) != 1 {
		t.Fatalf("List() after reopen returned %d configs, want 1", len(configs))
	}
	if configs[0].Name != "persist-test" {
		t.Errorf("config name = %q, want %q", configs[0].Name, "persist-test")
	}
	if configs[0].SrcQueue != "queue1" {
		t.Errorf("config src_queue = %q, want %q", configs[0].SrcQueue, "queue1")
	}
}

func TestStoreValidation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() error: %v", err)
	}

	tests := []struct {
		name string
		cfg  *ShovelConfig
	}{
		{
			name: "missing name",
			cfg:  &ShovelConfig{SrcURI: "amqp://x", SrcQueue: "q", DestURI: "amqp://y"},
		},
		{
			name: "missing src_uri",
			cfg:  &ShovelConfig{Name: "test", SrcQueue: "q", DestURI: "amqp://y"},
		},
		{
			name: "missing src_queue",
			cfg:  &ShovelConfig{Name: "test", SrcURI: "amqp://x", DestURI: "amqp://y"},
		},
		{
			name: "missing dest_uri",
			cfg:  &ShovelConfig{Name: "test", SrcURI: "amqp://x", SrcQueue: "q"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if addErr := store.Add(tt.cfg); addErr == nil {
				t.Error("Add() expected validation error, got nil")
			}
		})
	}
}

func TestStoreReplaceExisting(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() error: %v", err)
	}

	cfg1 := &ShovelConfig{
		Name:     "replace-me",
		SrcURI:   "amqp://src1:5672",
		SrcQueue: "q1",
		DestURI:  "amqp://dst1:5672",
		DestType: "amqp",
	}
	if err := store.Add(cfg1); err != nil {
		t.Fatalf("Add() error: %v", err)
	}

	cfg2 := &ShovelConfig{
		Name:     "replace-me",
		SrcURI:   "amqp://src2:5672",
		SrcQueue: "q2",
		DestURI:  "amqp://dst2:5672",
		DestType: "amqp",
	}
	if err := store.Add(cfg2); err != nil {
		t.Fatalf("Add() replace error: %v", err)
	}

	configs := store.List()
	if len(configs) != 1 {
		t.Fatalf("List() returned %d configs after replace, want 1", len(configs))
	}
	if configs[0].SrcQueue != "q2" {
		t.Errorf("SrcQueue = %q after replace, want %q", configs[0].SrcQueue, "q2")
	}
}

func TestStoreFederationLinks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() error: %v", err)
	}

	cfg := &FederationConfig{
		Name:     "upstream1",
		URI:      "amqp://remote:5672",
		Exchange: "events",
		MaxHops:  2,
		VHost:    "/",
	}

	if err := store.AddFederationLink(cfg); err != nil {
		t.Fatalf("AddFederationLink() error: %v", err)
	}

	links := store.ListFederationLinks()
	if len(links) != 1 {
		t.Fatalf("ListFederationLinks() returned %d, want 1", len(links))
	}
	if links[0].Name != "upstream1" {
		t.Errorf("link name = %q, want %q", links[0].Name, "upstream1")
	}

	// Check statuses.
	statuses := store.FederationLinkStatuses()
	if len(statuses) != 1 {
		t.Fatalf("FederationLinkStatuses() returned %d, want 1", len(statuses))
	}

	// Delete.
	if err := store.DeleteFederationLink("upstream1"); err != nil {
		t.Fatalf("DeleteFederationLink() error: %v", err)
	}

	links = store.ListFederationLinks()
	if len(links) != 0 {
		t.Errorf("ListFederationLinks() after delete returned %d, want 0", len(links))
	}
}

func TestStoreFederationPersistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() error: %v", err)
	}

	cfg := &FederationConfig{
		Name:     "fed-persist",
		URI:      "amqp://remote:5672",
		Exchange: "events",
		MaxHops:  3,
		VHost:    "/",
	}

	if err := store.AddFederationLink(cfg); err != nil {
		t.Fatalf("AddFederationLink() error: %v", err)
	}

	// Reopen.
	store2, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() reopen error: %v", err)
	}

	links := store2.ListFederationLinks()
	if len(links) != 1 {
		t.Fatalf("ListFederationLinks() after reopen returned %d, want 1", len(links))
	}
	if links[0].Name != "fed-persist" {
		t.Errorf("link name = %q, want %q", links[0].Name, "fed-persist")
	}
	if links[0].MaxHops != 3 {
		t.Errorf("max_hops = %d, want 3", links[0].MaxHops)
	}
}

func TestShovelConfigValidate(t *testing.T) {
	t.Parallel()

	valid := &ShovelConfig{
		Name:     "ok",
		SrcURI:   "amqp://x",
		SrcQueue: "q",
		DestURI:  "amqp://y",
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("Validate() unexpected error: %v", err)
	}
}

func TestStoreHTTPShovel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() error: %v", err)
	}

	cfg := &ShovelConfig{
		Name:       "http-shovel",
		SrcURI:     "amqp://source:5672",
		SrcQueue:   "src-q",
		DestURI:    "http://webhook.example.com/events",
		DestType:   "http",
		HTTPMethod: "POST",
	}

	if err := store.Add(cfg); err != nil {
		t.Fatalf("Add() error: %v", err)
	}

	configs := store.List()
	if len(configs) != 1 {
		t.Fatalf("List() returned %d, want 1", len(configs))
	}
	if configs[0].DestType != "http" {
		t.Errorf("DestType = %q, want %q", configs[0].DestType, "http")
	}
}
