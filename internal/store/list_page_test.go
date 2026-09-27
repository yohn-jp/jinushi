package store

import (
	"errors"
	"testing"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestListPageStableCursorAcrossInsert(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	for _, id := range []string{"run-c", "run-a", "run-b"} {
		if _, _, err := s.Create(testRun(id), nil); err != nil {
			t.Fatal(err)
		}
	}
	first, cursor, err := s.ListPage("", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].ID != "run-a" || first[1].ID != "run-b" || cursor != "run-b" {
		t.Fatalf("first page=%v cursor=%q", ids(first), cursor)
	}
	if _, _, err := s.Create(testRun("run-bb"), nil); err != nil {
		t.Fatal(err)
	}
	second, cursor, err := s.ListPage(cursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 || second[0].ID != "run-bb" || second[1].ID != "run-c" || cursor != "" {
		t.Fatalf("second page=%v cursor=%q", ids(second), cursor)
	}
	if _, _, err := s.ListPage("", 129); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("invalid page limit error = %v", err)
	}
}

func ids(runs []model.Run) []string {
	ids := make([]string, len(runs))
	for i := range runs {
		ids[i] = runs[i].ID
	}
	return ids
}
