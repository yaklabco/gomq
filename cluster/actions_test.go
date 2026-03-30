package cluster

import (
	"testing"
)

func TestEncodeDecodeActions(t *testing.T) {
	t.Parallel()

	t.Run("roundtrip preserves all action types", func(t *testing.T) {
		t.Parallel()

		actions := []Action{
			{Type: ActionAppend, Path: "seg/0001.dat", Data: []byte("appended data")},
			{Type: ActionReplace, Path: "defs.json", Data: []byte(`{"queues":[]}`)},
			{Type: ActionDelete, Path: "old/segment.dat"},
		}

		encoded, err := EncodeActions(actions)
		if err != nil {
			t.Fatalf("EncodeActions: %v", err)
		}

		decoded, err := DecodeActions(encoded)
		if err != nil {
			t.Fatalf("DecodeActions: %v", err)
		}

		if len(decoded) != len(actions) {
			t.Fatalf("got %d actions, want %d", len(decoded), len(actions))
		}

		for idx, want := range actions {
			got := decoded[idx]
			if got.Type != want.Type {
				t.Errorf("action[%d].Type = %d, want %d", idx, got.Type, want.Type)
			}
			if got.Path != want.Path {
				t.Errorf("action[%d].Path = %q, want %q", idx, got.Path, want.Path)
			}
			if string(got.Data) != string(want.Data) {
				t.Errorf("action[%d].Data = %q, want %q", idx, got.Data, want.Data)
			}
		}
	})

	t.Run("empty action list", func(t *testing.T) {
		t.Parallel()

		encoded, err := EncodeActions(nil)
		if err != nil {
			t.Fatalf("EncodeActions: %v", err)
		}

		decoded, err := DecodeActions(encoded)
		if err != nil {
			t.Fatalf("DecodeActions: %v", err)
		}

		if len(decoded) != 0 {
			t.Fatalf("got %d actions, want 0", len(decoded))
		}
	})

	t.Run("delete action has nil data", func(t *testing.T) {
		t.Parallel()

		actions := []Action{
			{Type: ActionDelete, Path: "removed.dat"},
		}

		encoded, err := EncodeActions(actions)
		if err != nil {
			t.Fatalf("EncodeActions: %v", err)
		}

		decoded, err := DecodeActions(encoded)
		if err != nil {
			t.Fatalf("DecodeActions: %v", err)
		}

		if decoded[0].Data != nil {
			t.Errorf("expected nil Data for delete, got %q", decoded[0].Data)
		}
	})

	t.Run("truncated data returns error", func(t *testing.T) {
		t.Parallel()

		// Encode a valid action, then truncate.
		actions := []Action{
			{Type: ActionAppend, Path: "test.dat", Data: []byte("data")},
		}
		encoded, err := EncodeActions(actions)
		if err != nil {
			t.Fatalf("EncodeActions: %v", err)
		}

		// Truncate to half.
		_, err = DecodeActions(encoded[:len(encoded)/2])
		if err == nil {
			t.Fatal("expected error for truncated data")
		}
	})
}
