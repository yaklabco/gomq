package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// INI key names shared across multiple sections.
const (
	iniKeyBind = "bind"
	iniKeyPort = "port"
)

// LoadFromFile reads an INI-style configuration file and returns a Config
// with defaults overridden by the file values. The file format matches
// LavinMQ's INI format with [main], [amqp], and [mgmt] sections.
func LoadFromFile(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config file %s: %w", path, err)
	}
	defer file.Close()

	cfg := Default()
	section := ""

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments.
		if line == "" || line[0] == '#' || line[0] == ';' {
			continue
		}

		// Section header.
		if line[0] == '[' && line[len(line)-1] == ']' {
			section = line[1 : len(line)-1]
			continue
		}

		// Key = value pair.
		key, value, ok := parseINILine(line)
		if !ok {
			continue
		}

		if err := applyConfigValue(cfg, section, key, value); err != nil {
			return nil, fmt.Errorf("config %s [%s] %s: %w", path, section, key, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}

	return cfg, nil
}

// parseINILine splits a line into key and value around the first '=' sign.
func parseINILine(line string) (string, string, bool) {
	idx := strings.IndexByte(line, '=')
	if idx < 0 {
		return "", "", false
	}

	key := strings.TrimSpace(line[:idx])
	value := strings.TrimSpace(line[idx+1:])

	return key, value, true
}

// applyConfigValue maps an INI section+key to the corresponding Config field.
func applyConfigValue(cfg *Config, section, key, value string) error {
	switch section {
	case "main":
		return applyMainSection(cfg, key, value)
	case "amqp":
		return applyAMQPSection(cfg, key, value)
	case "mgmt":
		return applyMgmtSection(cfg, key, value)
	case "mqtt":
		return applyMQTTSection(cfg, key, value)
	default:
		// Unknown sections are silently ignored for forward compatibility.
		return nil
	}
}

func applyMainSection(cfg *Config, key, value string) error {
	switch key {
	case "data_dir":
		cfg.DataDir = value
	case "log_level":
		// Stored for future use; currently a no-op.
	case "tls_cert":
		cfg.TLSCertFile = value
	case "tls_key":
		cfg.TLSKeyFile = value
	default:
		// Unknown keys are silently ignored for forward compatibility.
	}

	return nil
}

func applyAMQPSection(cfg *Config, key, value string) error {
	switch key {
	case iniKeyBind:
		cfg.AMQPBind = value
	case iniKeyPort:
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse port %q: %w", value, err)
		}
		cfg.AMQPPort = port
	case "tls_port":
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse tls_port %q: %w", value, err)
		}
		cfg.AMQPSPort = port
	case "heartbeat":
		secs, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse heartbeat %q: %w", value, err)
		}
		cfg.Heartbeat = time.Duration(secs) * time.Second
	default:
		// Unknown keys are silently ignored.
	}

	return nil
}

func applyMgmtSection(cfg *Config, key, value string) error {
	switch key {
	case iniKeyBind:
		cfg.HTTPBind = value
	case iniKeyPort:
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse port %q: %w", value, err)
		}
		cfg.HTTPPort = port
	default:
		// Unknown keys are silently ignored.
	}

	return nil
}

func applyMQTTSection(cfg *Config, key, value string) error {
	switch key {
	case iniKeyBind:
		cfg.MQTTBind = value
	case iniKeyPort:
		port, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("parse port %q: %w", value, err)
		}
		cfg.MQTTPort = port
	default:
		// Unknown keys are silently ignored.
	}

	return nil
}
