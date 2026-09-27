//go:build windows

package cli

import (
	"os"

	"golang.org/x/sys/windows"
)

func configureTerminal(input *os.File) (restore func(), rows, cols int, terminal bool, err error) {
	handle := windows.Handle(input.Fd())
	var original uint32
	if err := windows.GetConsoleMode(handle, &original); err != nil {
		return func() {}, 0, 0, false, nil
	}
	raw := original &^ (windows.ENABLE_PROCESSED_INPUT | windows.ENABLE_LINE_INPUT | windows.ENABLE_ECHO_INPUT)
	raw |= windows.ENABLE_VIRTUAL_TERMINAL_INPUT
	if err := windows.SetConsoleMode(handle, raw); err != nil {
		return func() {}, 0, 0, false, err
	}
	rows, cols, _ = terminalDimensions(input)
	return func() { _ = windows.SetConsoleMode(handle, original) }, rows, cols, true, nil
}

func terminalDimensions(input *os.File) (rows, cols int, ok bool) {
	var info windows.ConsoleScreenBufferInfo
	if err := windows.GetConsoleScreenBufferInfo(windows.Handle(input.Fd()), &info); err != nil {
		return 0, 0, false
	}
	rows = int(info.Window.Bottom-info.Window.Top) + 1
	cols = int(info.Window.Right-info.Window.Left) + 1
	return rows, cols, rows > 0 && cols > 0
}
