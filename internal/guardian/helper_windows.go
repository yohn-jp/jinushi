//go:build windows

package guardian

import (
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

func configureHelperCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.DETACHED_PROCESS | windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_NO_WINDOW}
}
