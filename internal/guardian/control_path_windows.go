//go:build windows

package guardian

import (
	"context"
	"net"

	"github.com/yohn-jp/jinushi/internal/ipc"
)

func listenControl(stateDir string) (net.Listener, error) { return ipc.Listen(stateDir) }

func dialControl(ctx context.Context, stateDir string) (net.Conn, error) {
	return ipc.Dial(ctx, stateDir)
}

func cleanupControl(string) {}
