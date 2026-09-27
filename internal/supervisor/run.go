package supervisor

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/store"
)

// Run starts one resident local supervisor. Opening the transactional store
// takes its exclusive process lock before any IPC endpoint becomes ready.
func Run(ctx context.Context, stateDir string) error {
	if stateDir == "" {
		return fmt.Errorf("state directory required")
	}
	root, err := filepath.Abs(stateDir)
	if err != nil {
		return fmt.Errorf("resolve state directory: %w", err)
	}
	if err := ensureStateDir(root); err != nil {
		return err
	}
	if err := secureStateDir(root); err != nil {
		return fmt.Errorf("restrict state directory: %w", err)
	}
	config, err := loadConfig(root)
	if err != nil {
		return err
	}
	db, err := store.Open(statePath(root), store.Options{
		EventRetentionCount: config.EventRetentionCount,
		EventRetentionBytes: config.EventRetentionBytes,
		OutputRetainedBytes: config.MaxOutputBytes,
	})
	if err != nil {
		return err
	}
	backend, err := newGuardedExecutor(root, config)
	if err != nil {
		_ = db.Close()
		return err
	}
	s := newService(root, db, backend, config)
	defer s.Close()
	if err := s.reconcile(); err != nil {
		return fmt.Errorf("reconcile Runs: %w", err)
	}
	s.startRetentionWorker()
	listener, err := ipc.Listen(root)
	if err != nil {
		return fmt.Errorf("listen local IPC: %w", err)
	}
	defer listener.Close()
	return ipc.ServeWithNotifier(ctx, listener, s.Handle, s.notifier)
}
