//go:build windows

package guardian

// Windows does not expose the same directory fsync operation as POSIX.
// Individual files are flushed before replacement; the helper's persisted
// evidence remains authoritative only when those writes complete.
func syncDir(string) error { return nil }
