package config

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"os"
	"time"
)

// Watch polls path every interval and invokes onChange with the newly loaded
// configuration whenever the file content changes and still validates. Invalid
// edits are logged and ignored so a typo never takes the gateway down.
//
// Polling (rather than inotify) keeps this working with Kubernetes ConfigMap
// mounts, which swap a symlink instead of writing the file in place. A change
// is applied only once the content has been stable for one interval, so a
// reload never observes a half-written (e.g. truncated) file.
func Watch(ctx context.Context, path string, interval time.Duration, logger *slog.Logger, onChange func(*Config)) {
	if path == "" || interval <= 0 {
		return
	}
	last := digest(path)
	var pending [32]byte
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		sum := digest(path)
		if sum == last {
			continue
		}
		if sum != pending {
			pending = sum // wait one more interval for the writer to finish
			continue
		}
		cfg, err := Load(path)
		if err != nil {
			logger.Warn("config reload rejected", "path", path, "error", err)
			last = sum
			continue
		}
		last = sum
		logger.Info("config reloaded", "path", path)
		onChange(cfg)
	}
}

func digest(path string) [32]byte {
	raw, err := os.ReadFile(path)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(raw)
}
