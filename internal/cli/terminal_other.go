//go:build !linux && !windows

package cli

import "os"

func configureTerminal(*os.File) (restore func(), rows, cols int, terminal bool, err error) {
	return func() {}, 0, 0, false, nil
}

func terminalDimensions(*os.File) (rows, cols int, ok bool) { return 0, 0, false }
