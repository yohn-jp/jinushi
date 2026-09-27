//go:build linux

package cli

import (
	"os"

	"golang.org/x/sys/unix"
)

func configureTerminal(input *os.File) (restore func(), rows, cols int, terminal bool, err error) {
	fd := int(input.Fd())
	termios, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return func() {}, 0, 0, false, nil
	}
	original := *termios
	raw := *termios
	raw.Iflag &^= unix.BRKINT | unix.ICRNL | unix.INPCK | unix.ISTRIP | unix.IXON
	raw.Oflag &^= unix.OPOST
	raw.Cflag &^= unix.CSIZE | unix.PARENB
	raw.Cflag |= unix.CS8
	raw.Lflag &^= unix.ECHO | unix.ICANON | unix.IEXTEN | unix.ISIG
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &raw); err != nil {
		return func() {}, 0, 0, false, err
	}
	winsize, _ := unix.IoctlGetWinsize(fd, unix.TIOCGWINSZ)
	if winsize != nil {
		rows, cols = int(winsize.Row), int(winsize.Col)
	}
	return func() { _ = unix.IoctlSetTermios(fd, unix.TCSETS, &original) }, rows, cols, true, nil
}

func terminalDimensions(input *os.File) (rows, cols int, ok bool) {
	winsize, err := unix.IoctlGetWinsize(int(input.Fd()), unix.TIOCGWINSZ)
	if err != nil || winsize == nil {
		return 0, 0, false
	}
	return int(winsize.Row), int(winsize.Col), winsize.Row > 0 && winsize.Col > 0
}
