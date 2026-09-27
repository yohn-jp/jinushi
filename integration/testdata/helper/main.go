// The helper is a real workload executable used by the blackbox suite. It
// intentionally has no Jinushi or backend imports so process behavior crosses
// the production IPC and OS backend boundary.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "mode required")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "spawn-grandchild":
		err = spawnGrandchild(os.Args[2:])
	case "wait":
		err = wait(os.Args[2:])
	case "flood":
		err = flood(os.Args[2:])
	case "allocate":
		err = allocate(os.Args[2:])
	case "signal-sink":
		err = signalSink(os.Args[2:])
	default:
		err = fmt.Errorf("unknown mode %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func signalSink(args []string) error {
	if len(args) != 0 {
		return errors.New("signal-sink takes no arguments")
	}
	received := make(chan os.Signal, 1)
	signal.Notify(received, syscall.SIGUSR1)
	defer signal.Stop(received)
	_, _ = fmt.Fprintln(os.Stdout, "signal-ready")
	for range received {
	}
	return nil
}

func spawnGrandchild(args []string) error {
	if len(args) != 1 {
		return errors.New("spawn-grandchild requires a PID file")
	}
	// Jinushi owns this helper, which owns the nested shell, which owns sleep.
	// The test records sleep's PID and proves it is no longer executable after
	// the Run reaches terminal state.
	child := exec.Command("/bin/sh", "-c", `sleep 60 & echo $! > "$1"; wait`, "jinushi-grandchild", args[0])
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		return fmt.Errorf("start intermediate child: %w", err)
	}
	// Keep the owned root live until cancellation reaches the entire session.
	for {
		time.Sleep(time.Minute)
	}
}

func wait(args []string) error {
	if len(args) != 1 {
		return errors.New("wait requires milliseconds")
	}
	milliseconds, err := strconv.Atoi(args[0])
	if err != nil || milliseconds < 0 {
		return errors.New("invalid wait duration")
	}
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
	return nil
}

func flood(args []string) error {
	if len(args) != 1 {
		return errors.New("flood requires bytes per stream")
	}
	count, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || count <= 0 {
		return errors.New("invalid flood byte count")
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, output := range []*os.File{os.Stdout, os.Stderr} {
		wg.Add(1)
		go func(output *os.File) {
			defer wg.Done()
			errs <- writeZeros(output, count)
		}(output)
	}
	wg.Wait()
	close(errs)
	for writeErr := range errs {
		if writeErr != nil {
			return writeErr
		}
	}
	return nil
}

func writeZeros(writer io.Writer, count int64) error {
	block := make([]byte, 32<<10)
	for written := int64(0); written < count; {
		next := int64(len(block))
		if count-written < next {
			next = count - written
		}
		chunk := block[:int(next)]
		for len(chunk) > 0 {
			n, err := writer.Write(chunk)
			written += int64(n)
			chunk = chunk[n:]
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
		}
	}
	return nil
}

func allocate(args []string) error {
	if len(args) != 1 {
		return errors.New("allocate requires bytes")
	}
	count, err := strconv.Atoi(args[0])
	if err != nil || count <= 0 {
		return errors.New("invalid allocation size")
	}
	memory := make([]byte, count)
	for offset := 0; offset < len(memory); offset += 4096 {
		memory[offset] = byte(offset)
	}
	fmt.Fprintf(os.Stdout, "allocated %d bytes\n", len(memory))
	time.Sleep(time.Minute)
	return nil
}
