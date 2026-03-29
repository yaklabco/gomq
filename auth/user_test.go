package auth

import (
	"encoding/json"
	"testing"
)

func TestHasTag(t *testing.T) {
	t.Parallel()

	user := &User{
		Name: "admin",
		Tags: []string{"administrator", "management"},
	}

	tests := []struct {
		name string
		tag  string
		want bool
	}{
		{name: "has administrator", tag: "administrator", want: true},
		{name: "has management", tag: "management", want: true},
		{name: "missing monitoring", tag: "monitoring", want: false},
		{name: "empty string", tag: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := user.HasTag(tt.tag); got != tt.want {
				t.Errorf("HasTag(%q) = %v, want %v", tt.tag, got, tt.want)
			}
		})
	}
}

func TestCheckPermission(t *testing.T) {
	t.Parallel()

	user := &User{
		Name: "guest",
		Permissions: map[string]Permission{
			"/": {Configure: ".*", Read: ".*", Write: ".*"},
			"prod": {
				Configure: "^$",
				Read:      "^amq\\.",
				Write:     "^amq\\.",
			},
		},
	}

	tests := []struct {
		name     string
		vhost    string
		resource string
		action   string
		want     bool
	}{
		{name: "root configure anything", vhost: "/", resource: "my.queue", action: "configure", want: true},
		{name: "root read anything", vhost: "/", resource: "my.exchange", action: "read", want: true},
		{name: "root write anything", vhost: "/", resource: "test", action: "write", want: true},
		{name: "prod configure denied", vhost: "prod", resource: "my.queue", action: "configure", want: false},
		{name: "prod configure empty allowed", vhost: "prod", resource: "", action: "configure", want: true},
		{name: "prod read amq prefix", vhost: "prod", resource: "amq.direct", action: "read", want: true},
		{name: "prod read non-amq denied", vhost: "prod", resource: "my.exchange", action: "read", want: false},
		{name: "unknown vhost denied", vhost: "missing", resource: "test", action: "read", want: false},
		{name: "unknown action denied", vhost: "/", resource: "test", action: "delete", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := user.CheckPermission(tt.vhost, tt.resource, tt.action); got != tt.want {
				t.Errorf("CheckPermission(%q, %q, %q) = %v, want %v",
					tt.vhost, tt.resource, tt.action, got, tt.want)
			}
		})
	}
}

func TestSetPermission(t *testing.T) {
	t.Parallel()

	user := &User{
		Name:        "guest",
		Permissions: map[string]Permission{},
	}

	user.SetPermission("/", Permission{Configure: ".*", Read: ".*", Write: ".*"})

	perm, ok := user.Permissions["/"]
	if !ok {
		t.Fatal("SetPermission() did not add permission for vhost /")
	}

	if perm.Configure != ".*" {
		t.Errorf("Configure = %q, want %q", perm.Configure, ".*")
	}

	// Update existing permission.
	user.SetPermission("/", Permission{Configure: "^$", Read: "^$", Write: "^$"})

	perm = user.Permissions["/"]
	if perm.Configure != "^$" {
		t.Errorf("after update: Configure = %q, want %q", perm.Configure, "^$")
	}
}

func TestUserJSONRoundtrip(t *testing.T) {
	t.Parallel()

	original := &User{
		Name:     "testuser",
		Password: HashPassword("secret"),
		Tags:     []string{"administrator"},
		Permissions: map[string]Permission{
			"/": {Configure: ".*", Read: ".*", Write: ".*"},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	var restored User
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	if restored.Name != original.Name {
		t.Errorf("Name = %q, want %q", restored.Name, original.Name)
	}

	if !restored.Password.Verify("secret") {
		t.Error("restored password does not verify with original plaintext")
	}

	if len(restored.Tags) != 1 || restored.Tags[0] != "administrator" {
		t.Errorf("Tags = %v, want [administrator]", restored.Tags)
	}

	if _, ok := restored.Permissions["/"]; !ok {
		t.Error("Permissions missing vhost /")
	}
}
