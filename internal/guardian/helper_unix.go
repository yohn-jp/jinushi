//go:build !windows

package guardian

import (
	"os/exec"
	"syscall"
)

func configureHelperCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
