//go:build !windows && !linux

package supervisor

import "os"

func secureStateDir(root string) error { return os.Chmod(root, 0700) }
