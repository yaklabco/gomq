package cluster

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewFileIndex(t *testing.T) {
	t.Parallel()

	t.Run("scans directory and computes checksums", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()

		// Create some files.
		writeFile(t, filepath.Join(dir, "a.dat"), []byte("hello"))
		writeFile(t, filepath.Join(dir, "sub", "b.dat"), []byte("world"))

		fi, err := NewFileIndex(dir)
		if err != nil {
			t.Fatalf("NewFileIndex(%q): %v", dir, err)
		}

		crc, ok := fi.Checksum("a.dat")
		if !ok {
			t.Fatal("expected checksum for a.dat")
		}
		if crc == 0 {
			t.Fatal("expected non-zero checksum for a.dat")
		}

		crc2, ok := fi.Checksum(filepath.Join("sub", "b.dat"))
		if !ok {
			t.Fatal("expected checksum for sub/b.dat")
		}
		if crc2 == 0 {
			t.Fatal("expected non-zero checksum for sub/b.dat")
		}
	})

	t.Run("empty directory produces empty index", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()

		fi, err := NewFileIndex(dir)
		if err != nil {
			t.Fatalf("NewFileIndex(%q): %v", dir, err)
		}

		if _, ok := fi.Checksum("nope"); ok {
			t.Fatal("expected no checksum for missing file")
		}
	})

	t.Run("nonexistent directory returns error", func(t *testing.T) {
		t.Parallel()

		_, err := NewFileIndex("/no/such/directory/ever")
		if err == nil {
			t.Fatal("expected error for nonexistent directory")
		}
	})
}

func TestFileIndex_Update(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "f.dat"), []byte("original"))

	fi, err := NewFileIndex(dir)
	if err != nil {
		t.Fatalf("NewFileIndex: %v", err)
	}

	oldCRC, _ := fi.Checksum("f.dat")

	// Modify the file and update.
	writeFile(t, filepath.Join(dir, "f.dat"), []byte("modified"))

	if err := fi.Update("f.dat"); err != nil {
		t.Fatalf("Update: %v", err)
	}

	newCRC, ok := fi.Checksum("f.dat")
	if !ok {
		t.Fatal("expected checksum after update")
	}
	if newCRC == oldCRC {
		t.Fatal("expected checksum to change after file modification")
	}
}

func TestFileIndex_Remove(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "f.dat"), []byte("data"))

	fi, err := NewFileIndex(dir)
	if err != nil {
		t.Fatalf("NewFileIndex: %v", err)
	}

	fi.Remove("f.dat")

	if _, ok := fi.Checksum("f.dat"); ok {
		t.Fatal("expected no checksum after Remove")
	}
}

func TestFileIndex_Diff(t *testing.T) {
	t.Parallel()

	t.Run("detects files to send and delete", func(t *testing.T) {
		t.Parallel()

		dirA := t.TempDir()
		writeFile(t, filepath.Join(dirA, "same.dat"), []byte("identical"))
		writeFile(t, filepath.Join(dirA, "changed.dat"), []byte("version-a"))
		writeFile(t, filepath.Join(dirA, "only-a.dat"), []byte("only on a"))

		dirB := t.TempDir()
		writeFile(t, filepath.Join(dirB, "same.dat"), []byte("identical"))
		writeFile(t, filepath.Join(dirB, "changed.dat"), []byte("version-b"))
		writeFile(t, filepath.Join(dirB, "only-b.dat"), []byte("only on b"))

		fiA, err := NewFileIndex(dirA)
		if err != nil {
			t.Fatalf("NewFileIndex(A): %v", err)
		}
		fiB, err := NewFileIndex(dirB)
		if err != nil {
			t.Fatalf("NewFileIndex(B): %v", err)
		}

		toSend, toDelete := fiA.Diff(fiB)

		// toSend: files in A that are missing or different in B.
		sendSet := toSet(toSend)
		if !sendSet["changed.dat"] {
			t.Error("expected changed.dat in toSend")
		}
		if !sendSet["only-a.dat"] {
			t.Error("expected only-a.dat in toSend")
		}
		if sendSet["same.dat"] {
			t.Error("same.dat should not be in toSend")
		}

		// toDelete: files in B that are not in A.
		delSet := toSet(toDelete)
		if !delSet["only-b.dat"] {
			t.Error("expected only-b.dat in toDelete")
		}
		if delSet["same.dat"] {
			t.Error("same.dat should not be in toDelete")
		}
	})

	t.Run("identical indices produce no diff", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "a.dat"), []byte("data"))

		fi, err := NewFileIndex(dir)
		if err != nil {
			t.Fatalf("NewFileIndex: %v", err)
		}

		toSend, toDelete := fi.Diff(fi)
		if len(toSend) != 0 {
			t.Errorf("expected empty toSend, got %v", toSend)
		}
		if len(toDelete) != 0 {
			t.Errorf("expected empty toDelete, got %v", toDelete)
		}
	})
}

// writeFile creates a file with the given data, creating parent directories.
func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func toSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}
