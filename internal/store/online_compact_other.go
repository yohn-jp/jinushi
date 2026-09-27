//go:build !linux

package store

func compactOnlineLocked(*Store) error {
	return ErrCompactionUnsupported
}

func cleanupCompactionArtifacts(string) {}
