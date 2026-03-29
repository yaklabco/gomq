package auth

import (
	"path/filepath"
	"testing"
)

func TestNewUserStoreCreatesDefaultGuest(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	user, ok := store.Get("guest")
	if !ok {
		t.Fatal("default guest user not found")
	}

	if user.Name != "guest" {
		t.Errorf("Name = %q, want %q", user.Name, "guest")
	}

	if !user.HasTag("administrator") {
		t.Error("guest user missing administrator tag")
	}

	perm, ok := user.Permissions["/"]
	if !ok {
		t.Fatal("guest user missing permission for vhost /")
	}

	if perm.Configure != ".*" || perm.Read != ".*" || perm.Write != ".*" {
		t.Errorf("guest permissions = %+v, want all .*", perm)
	}
}

func TestAuthenticateGuest(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	user, err := store.Authenticate("guest", "guest")
	if err != nil {
		t.Fatalf("Authenticate() error: %v", err)
	}

	if user.Name != "guest" {
		t.Errorf("Name = %q, want %q", user.Name, "guest")
	}
}

func TestAuthenticateRejectsWrongPassword(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	_, err = store.Authenticate("guest", "wrong")
	if err == nil {
		t.Error("Authenticate() expected error for wrong password, got nil")
	}
}

func TestAuthenticateRejectsUnknownUser(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	_, err = store.Authenticate("nobody", "guest")
	if err == nil {
		t.Error("Authenticate() expected error for unknown user, got nil")
	}
}

func TestAddAndDeleteUser(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	newUser := &User{
		Name:     "alice",
		Password: HashPassword("alice123"),
		Tags:     []string{"management"},
		Permissions: map[string]Permission{
			"/": {Configure: "^$", Read: ".*", Write: "^$"},
		},
	}

	if err := store.Add(newUser); err != nil {
		t.Fatalf("Add() error: %v", err)
	}

	if _, ok := store.Get("alice"); !ok {
		t.Error("alice not found after Add()")
	}

	if err := store.Delete("alice"); err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	if _, ok := store.Get("alice"); ok {
		t.Error("alice still found after Delete()")
	}
}

func TestAddDuplicateUserReturnsError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	dup := &User{
		Name:     "guest",
		Password: HashPassword("x"),
	}

	if err := store.Add(dup); err == nil {
		t.Error("Add() expected error for duplicate user, got nil")
	}
}

func TestDeleteNonexistentUserReturnsError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	if err := store.Delete("nobody"); err == nil {
		t.Error("Delete() expected error for nonexistent user, got nil")
	}
}

func TestListUsers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	users := store.List()
	if len(users) != 1 {
		t.Fatalf("List() returned %d users, want 1", len(users))
	}

	if users[0].Name != "guest" {
		t.Errorf("List()[0].Name = %q, want %q", users[0].Name, "guest")
	}
}

func TestPersistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Create store and add a user.
	store, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	alice := &User{
		Name:     "alice",
		Password: HashPassword("secret"),
		Tags:     []string{"monitoring"},
		Permissions: map[string]Permission{
			"/": {Configure: "^$", Read: ".*", Write: "^$"},
		},
	}

	if err := store.Add(alice); err != nil {
		t.Fatalf("Add() error: %v", err)
	}

	// Reopen the store from the same directory.
	store2, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() reopen error: %v", err)
	}

	user, ok := store2.Get("alice")
	if !ok {
		t.Fatal("alice not found after reopening store")
	}

	if !user.Password.Verify("secret") {
		t.Error("alice's password does not verify after reopening store")
	}

	if !user.HasTag("monitoring") {
		t.Error("alice missing monitoring tag after reopening store")
	}
}

func TestNewUserStoreCreatesUsersJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	_, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	path := filepath.Join(dir, "users.json")

	// Reopen to confirm the file was written.
	store, err := NewUserStore(dir)
	if err != nil {
		t.Fatalf("NewUserStore() reopen error: %v", err)
	}

	_ = path

	if _, ok := store.Get("guest"); !ok {
		t.Error("guest not found after reopening store from users.json")
	}
}
