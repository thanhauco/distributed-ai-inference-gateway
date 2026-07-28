package config

import (
	"path/filepath"
	"runtime"
	"testing"
)

// TestShippedConfigsLoad keeps the example configs in sync with the schema.
func TestShippedConfigsLoad(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..", "configs")
	for _, name := range []string{"gateway.yaml"} {
		cfg, err := Load(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(cfg.Workers) == 0 || len(cfg.Models) == 0 {
			t.Fatalf("%s: expected workers and models", name)
		}
	}
}
