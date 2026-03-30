package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// UserStore manages users persisted as JSON on disk.
type UserStore struct {
	mu    sync.RWMutex
	users map[string]*User
	path  string // JSON file path
}

// NewUserStore loads users from dataDir/users.json, or creates a default store
// with a guest user if the file does not exist.
func NewUserStore(dataDir string) (*UserStore, error) {
	st := &UserStore{
		users: make(map[string]*User),
		path:  filepath.Join(dataDir, "users.json"),
	}

	if err := st.load(); err != nil {
		return nil, fmt.Errorf("load user store: %w", err)
	}

	return st, nil
}

// Get returns the user with the given name and true, or nil and false if not found.
func (s *UserStore) Get(name string) (*User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, ok := s.users[name]

	return user, ok
}

// Authenticate finds the named user and verifies the password.
// It returns the user on success or an error if the user is not found or the
// password is incorrect.
func (s *UserStore) Authenticate(username, password string) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, ok := s.users[username]
	if !ok {
		return nil, fmt.Errorf("authenticate: user %q not found", username)
	}

	if !user.Password.Verify(password) {
		return nil, fmt.Errorf("authenticate: invalid password for user %q", username)
	}

	return user, nil
}

// Add inserts a new user and persists the store to disk.
// It returns an error if a user with the same name already exists.
func (s *UserStore) Add(user *User) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[user.Name]; exists {
		return fmt.Errorf("add user: %q already exists", user.Name)
	}

	s.users[user.Name] = user

	if err := s.save(); err != nil {
		delete(s.users, user.Name)

		return fmt.Errorf("add user: %w", err)
	}

	return nil
}

// Put creates or updates a user and persists the store to disk.
func (s *UserStore) Put(user *User) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.users[user.Name] = user

	if err := s.save(); err != nil {
		return fmt.Errorf("put user: %w", err)
	}

	return nil
}

// Delete removes the named user and persists the store to disk.
// It returns an error if the user does not exist.
func (s *UserStore) Delete(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	user, ok := s.users[name]
	if !ok {
		return fmt.Errorf("delete user: %q not found", name)
	}

	delete(s.users, name)

	if err := s.save(); err != nil {
		s.users[name] = user

		return fmt.Errorf("delete user: %w", err)
	}

	return nil
}

// List returns all users in the store.
func (s *UserStore) List() []*User {
	s.mu.RLock()
	defer s.mu.RUnlock()

	users := make([]*User, 0, len(s.users))
	for _, user := range s.users {
		users = append(users, user)
	}

	return users
}

// load reads users from the JSON file, or creates defaults if the file does not exist.
func (s *UserStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.createDefaultUser()

		return s.save()
	}

	if err != nil {
		return fmt.Errorf("read %s: %w", s.path, err)
	}

	var users []*User
	if err := json.Unmarshal(data, &users); err != nil {
		return fmt.Errorf("parse %s: %w", s.path, err)
	}

	for _, user := range users {
		s.users[user.Name] = user
	}

	return nil
}

// createDefaultUser adds the default guest user with administrator privileges.
func (s *UserStore) createDefaultUser() {
	guest := &User{
		Name:     "guest",
		Password: HashPassword("guest"),
		Tags:     []string{"administrator"},
		Permissions: map[string]Permission{
			"/": {Configure: ".*", Read: ".*", Write: ".*"},
		},
	}
	s.users["guest"] = guest
}

// save writes all users to disk atomically via a temporary file rename.
func (s *UserStore) save() error {
	data, err := json.MarshalIndent(s.userSlice(), "", "  ")
	if err != nil {
		return fmt.Errorf("marshal users: %w", err)
	}

	tmp := s.path + ".tmp"

	if err := os.WriteFile(tmp, data, 0o644); err != nil { //nolint:gosec // world-readable config file
		return fmt.Errorf("write %s: %w", tmp, err)
	}

	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename %s to %s: %w", tmp, s.path, err)
	}

	return nil
}

// userSlice returns all users as a slice (must be called with lock held).
func (s *UserStore) userSlice() []*User {
	users := make([]*User, 0, len(s.users))
	for _, user := range s.users {
		users = append(users, user)
	}

	return users
}
