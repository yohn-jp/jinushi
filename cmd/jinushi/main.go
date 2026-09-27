package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yohn-jp/jinushi/internal/cli"
	"github.com/yohn-jp/jinushi/internal/guardian"
	"github.com/yohn-jp/jinushi/internal/supervisor"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "__guardian" {
		if err := guardian.ServeFromArgs(os.Args[2:], supervisor.PlatformBackendFactory()); err != nil {
			fmt.Fprintf(os.Stderr, "guardian: %v\n", err)
			os.Exit(1)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	code := cli.Main(ctx, os.Args[1:], os.Stdout, os.Stderr, supervisor.Run)
	os.Exit(code)
}
