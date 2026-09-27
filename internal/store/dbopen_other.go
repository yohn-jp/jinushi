//go:build !linux

package store

import (
	"os"

	bolt "go.etcd.io/bbolt"
)

// openDatabase preserves the existing experimental non-Linux store behavior.
func openDatabase(path string, mode os.FileMode, options *bolt.Options) (*bolt.DB, *os.File, error) {
	db, err := bolt.Open(path, mode, options)
	return db, nil, err
}
