// Command gomq runs the GoMQ AMQP 0-9-1 message broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/jamesainslie/gomq"
	"github.com/jamesainslie/gomq/config"
)

func main() {
	cfg := config.Default()

	var configFile string

	flag.StringVar(&configFile, "config", "", "Path to INI config file")
	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Data directory for persistent storage")
	flag.StringVar(&cfg.AMQPBind, "bind", cfg.AMQPBind, "AMQP bind address")
	flag.IntVar(&cfg.AMQPPort, "port", cfg.AMQPPort, "AMQP listen port")
	flag.StringVar(&cfg.HTTPBind, "http-bind", cfg.HTTPBind, "HTTP management API bind address")
	flag.IntVar(&cfg.HTTPPort, "http-port", cfg.HTTPPort, "HTTP management API port (-1 to disable)")
	flag.StringVar(&cfg.TLSCertFile, "tls-cert", cfg.TLSCertFile, "TLS certificate file path")
	flag.StringVar(&cfg.TLSKeyFile, "tls-key", cfg.TLSKeyFile, "TLS private key file path")
	flag.IntVar(&cfg.AMQPSPort, "amqps-port", cfg.AMQPSPort, "AMQPS (TLS) listen port (-1 to disable)")
	flag.StringVar(&cfg.MQTTBind, "mqtt-bind", cfg.MQTTBind, "MQTT bind address")
	flag.IntVar(&cfg.MQTTPort, "mqtt-port", cfg.MQTTPort, "MQTT listen port (-1 to disable)")
	flag.Parse()

	// Load config file first, then CLI flags override.
	if configFile != "" {
		fileCfg, err := config.LoadFromFile(configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		// Apply CLI flags over file config: re-parse flags with file config as base.
		cfg = fileCfg
		flag.Visit(func(f *flag.Flag) {
			applyFlag(cfg, f)
		})
	}

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// applyFlag applies a single CLI flag value to the config, overriding
// the file-loaded value.
func applyFlag(cfg *config.Config, flg *flag.Flag) {
	switch flg.Name {
	case "data-dir":
		cfg.DataDir = flg.Value.String()
	case "bind":
		cfg.AMQPBind = flg.Value.String()
	case "port":
		if port, err := strconv.Atoi(flg.Value.String()); err == nil {
			cfg.AMQPPort = port
		}
	case "http-bind":
		cfg.HTTPBind = flg.Value.String()
	case "http-port":
		if port, err := strconv.Atoi(flg.Value.String()); err == nil {
			cfg.HTTPPort = port
		}
	case "tls-cert":
		cfg.TLSCertFile = flg.Value.String()
	case "tls-key":
		cfg.TLSKeyFile = flg.Value.String()
	case "amqps-port":
		if port, err := strconv.Atoi(flg.Value.String()); err == nil {
			cfg.AMQPSPort = port
		}
	case "mqtt-bind":
		cfg.MQTTBind = flg.Value.String()
	case "mqtt-port":
		if port, err := strconv.Atoi(flg.Value.String()); err == nil {
			cfg.MQTTPort = port
		}
	}
}

func run(cfg *config.Config) error {
	opts := []gomq.Option{
		gomq.WithDataDir(cfg.DataDir),
		gomq.WithAMQPPort(cfg.AMQPPort),
		gomq.WithBind(cfg.AMQPBind),
		gomq.WithHTTPPort(cfg.HTTPPort),
		gomq.WithHTTPBind(cfg.HTTPBind),
	}

	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		opts = append(opts, gomq.WithTLS(cfg.TLSCertFile, cfg.TLSKeyFile))
		opts = append(opts, gomq.WithAMQPSPort(cfg.AMQPSPort))
	}

	opts = append(opts, gomq.WithMQTTPort(cfg.MQTTPort))
	opts = append(opts, gomq.WithMQTTBind(cfg.MQTTBind))

	brk, err := gomq.New(opts...)
	if err != nil {
		return fmt.Errorf("create broker: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "gomq starting (data_dir=%s, amqp=%s:%d, http=%s:%d, mqtt=%s:%d)\n",
		cfg.DataDir, cfg.AMQPBind, cfg.AMQPPort, cfg.HTTPBind, cfg.HTTPPort, cfg.MQTTBind, cfg.MQTTPort)

	if err := brk.ListenAndServe(ctx); err != nil {
		return fmt.Errorf("serve: %w", err)
	}

	if err := brk.Close(); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}

	fmt.Fprintln(os.Stderr, "gomq stopped")
	return nil
}
