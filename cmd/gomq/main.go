// Command gomq runs the GoMQ AMQP 0-9-1 message broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jamesainslie/gomq"
	"github.com/jamesainslie/gomq/config"
)

func main() {
	cfg := config.Default()

	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Data directory for persistent storage")
	flag.StringVar(&cfg.AMQPBind, "bind", cfg.AMQPBind, "AMQP bind address")
	flag.IntVar(&cfg.AMQPPort, "port", cfg.AMQPPort, "AMQP listen port")
	flag.Parse()

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cfg *config.Config) error {
	brk, err := gomq.New(
		gomq.WithDataDir(cfg.DataDir),
		gomq.WithAMQPPort(cfg.AMQPPort),
		gomq.WithBind(cfg.AMQPBind),
	)
	if err != nil {
		return fmt.Errorf("create broker: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "gomq starting (data_dir=%s, amqp=%s:%d)\n",
		cfg.DataDir, cfg.AMQPBind, cfg.AMQPPort)

	if err := brk.ListenAndServe(ctx); err != nil {
		return fmt.Errorf("serve: %w", err)
	}

	if err := brk.Close(); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}

	fmt.Fprintln(os.Stderr, "gomq stopped")
	return nil
}
