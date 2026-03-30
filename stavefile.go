//go:build stave

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/yaklabco/stave/pkg/sh"
	"github.com/yaklabco/stave/pkg/st"
)

// Default runs all targets.
var Default = All //nolint:gochecknoglobals // required by stave

// Aliases provides shorthand names for common targets.
var Aliases = map[string]any{ //nolint:gochecknoglobals // required by stave
	"b": Build,
	"t": Test.All,
	"l": Lint.All,
	"i": Install,
}

// All runs init, then lint and test in parallel, then build.
func All() {
	st.Deps(Init)
	st.Deps(Lint.All, Test.All)
	st.Deps(Build)
}

// Init tidies modules and runs code generation.
func Init() error {
	gocmd := st.GoCmd()
	if err := sh.Run(gocmd, "mod", "tidy"); err != nil {
		return err
	}
	return sh.Run(gocmd, "generate", "./...")
}

// Build compiles the gomq binary to bin/gomq.
func Build() error {
	return sh.RunV(st.GoCmd(), "build", "-o", "bin/gomq", "./cmd/gomq")
}

// Install builds and installs gomq to GOBIN.
func Install() error {
	name := "gomq"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	gocmd := st.GoCmd()
	bin, err := sh.Output(gocmd, "env", "GOBIN")
	if err != nil {
		return fmt.Errorf("can't determine GOBIN: %w", err)
	}
	if bin == "" {
		gopath, err := sh.Output(gocmd, "env", "GOPATH")
		if err != nil {
			return fmt.Errorf("can't determine GOPATH: %w", err)
		}
		paths := strings.Split(gopath, string([]rune{os.PathListSeparator}))
		bin = filepath.Join(paths[0], "bin")
	}
	if err := os.Mkdir(bin, 0o700); err != nil && !os.IsExist(err) {
		return fmt.Errorf("failed to create %q: %w", bin, err)
	}
	return sh.RunV(gocmd, "build", "-o", filepath.Join(bin, name), "./cmd/gomq")
}

// Uninstall removes gomq from GOBIN.
func Uninstall() error {
	name := "gomq"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	gocmd := st.GoCmd()
	bin, err := sh.Output(gocmd, "env", "GOBIN")
	if err != nil {
		return fmt.Errorf("can't determine GOBIN: %w", err)
	}
	if bin == "" {
		gopath, err := sh.Output(gocmd, "env", "GOPATH")
		if err != nil {
			return fmt.Errorf("can't determine GOPATH: %w", err)
		}
		paths := strings.Split(gopath, string([]rune{os.PathListSeparator}))
		bin = filepath.Join(paths[0], "bin")
	}
	return os.Remove(filepath.Join(bin, name))
}

// Clean removes the bin/ directory.
func Clean() error {
	return sh.Rm("bin/")
}

// Lint groups linting targets.
type Lint st.Namespace

// All runs all linters.
func (Lint) All() {
	st.Deps(Lint.Go)
}

// Go runs golangci-lint with auto-fix, then validates.
func (Lint) Go() error {
	_ = sh.Run("golangci-lint", "run", "--fix") //nolint:errcheck // intentional: fix first, verify next
	return sh.RunV("golangci-lint", "run")
}

// Test groups testing targets.
type Test st.Namespace

// All runs all test suites.
func (Test) All() {
	st.Deps(Test.Go)
}

// Go runs Go tests with race detection and coverage.
func (Test) Go() error {
	return sh.RunV("gotestsum",
		"-f", "pkgname-and-test-fails",
		"--",
		"-v", "-race", "-cover", "./...",
		"-count", "1",
		"-timeout", "120s",
		"-coverprofile=coverage.out",
		"-covermode=atomic",
	)
}

// Gate runs the full test suite including correctness guarantees and
// performance regression gates.
func (Test) Gate() error {
	return sh.RunV("gotestsum",
		"-f", "pkgname-and-test-fails",
		"--",
		"-v", "./...",
		"-count", "1",
		"-timeout", "120s",
		"-run", "TestGuarantee|TestPerfGate|TestIntegration",
	)
}

// Release groups release targets.
type Release st.Namespace

// Publish tags the release and runs goreleaser.
func (Release) Publish() error {
	version, err := sh.Output("svu", "next")
	if err != nil {
		return fmt.Errorf("determine next version: %w", err)
	}
	if err := sh.RunV("git", "tag", "-a", version, "-m", version); err != nil {
		return fmt.Errorf("tag %s: %w", version, err)
	}
	if err := sh.RunV("git", "push", "origin", version); err != nil {
		return fmt.Errorf("push tag %s: %w", version, err)
	}
	return sh.RunV("goreleaser", "release", "--clean")
}

// Fmt groups formatting targets.
type Fmt st.Namespace

// Go formats Go source files with gofmt and goimports.
func (Fmt) Go() error {
	if err := sh.Run("gofmt", "-w", "."); err != nil {
		return err
	}
	return sh.Run("goimports", "-w", ".")
}
