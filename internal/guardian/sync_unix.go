//go:build !windows

package guardian

import (
	"os"
)

func secureDir(path string) error { return os.Chmod(path, 0700) }

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
