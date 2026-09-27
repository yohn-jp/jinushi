package store

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/yohn-jp/jinushi/internal/model"
	bolt "go.etcd.io/bbolt"
)

// ListPage returns Runs after the supplied Run ID in stable ID order. The
// cursor is the last returned ID only when another row remains.
func (s *Store) ListPage(after string, limit int) ([]model.Run, string, error) {
	if limit < 1 || limit > 128 {
		return nil, "", fmt.Errorf("%w: list limit must be between 1 and 128", ErrInvalidRun)
	}
	runs := make([]model.Run, 0, limit)
	var next string
	err := s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte(runsBucketName)).Cursor()
		var key, value []byte
		if after == "" {
			key, value = cursor.First()
		} else {
			key, value = cursor.Seek([]byte(after))
			if key != nil && bytes.Equal(key, []byte(after)) {
				key, value = cursor.Next()
			}
		}
		for key != nil && len(runs) < limit {
			var run model.Run
			if err := json.Unmarshal(value, &run); err != nil {
				return fmt.Errorf("decode Run %q: %w", key, err)
			}
			runs = append(runs, run)
			key, value = cursor.Next()
		}
		if key != nil && len(runs) > 0 {
			next = runs[len(runs)-1].ID
		}
		return nil
	})
	return runs, next, err
}
