// Package config provides broker configuration with LavinMQ-compatible defaults.
package config

import "time"

// Default configuration values matching LavinMQ.
const (
	DefaultDataDir                     = "/var/lib/gomq"
	DefaultAMQPBind                    = "127.0.0.1"
	DefaultAMQPPort                    = 5672
	DefaultAMQPSPort                   = -1 // disabled by default, 5671 when TLS is configured
	DefaultMQTTBind                    = "127.0.0.1"
	DefaultMQTTPort                    = 1883
	DefaultHTTPBind                    = "127.0.0.1"
	DefaultHTTPPort                    = 15672
	DefaultHeartbeat                   = 300 * time.Second
	DefaultFrameMax                    = 131072
	DefaultChannelMax                  = 2048
	DefaultMaxMessageSize              = 128 * 1024 * 1024
	DefaultSegmentSize                 = 8 * 1024 * 1024
	DefaultSocketBufferSize            = 16384
	DefaultConsumerPrefetch     uint16 = 65535
	DefaultTCPKeepaliveIdle            = 60 * time.Second
	DefaultTCPKeepaliveInterval        = 10 * time.Second
	DefaultTCPKeepaliveCount           = 3
	DefaultUser                        = "guest"
	DefaultPasswordHash                = "+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+" //nolint:gosec // not a credential, this is the SHA256 hash of "guest"
)

// Config holds all broker configuration.
type Config struct {
	DataDir                 string
	AMQPBind                string
	AMQPPort                int
	AMQPSPort               int // AMQPS (TLS) port; -1 to disable
	MQTTBind                string
	MQTTPort                int // MQTT port; -1 to disable
	TLSCertFile             string
	TLSKeyFile              string
	HTTPBind                string
	HTTPPort                int
	Heartbeat               time.Duration
	FrameMax                uint32
	ChannelMax              uint16
	MaxMessageSize          uint32
	SegmentSize             int64
	SocketBufferSize        int
	DefaultConsumerPrefetch uint16
	TCPNodelay              bool
	TCPKeepalive            bool
	TCPKeepaliveIdle        time.Duration
	TCPKeepaliveInterval    time.Duration
	TCPKeepaliveCount       int
	FreeDiskMin             int64
	SetTimestamp            bool
	DefaultUser             string
	DefaultPasswordHash     string
}

// Default returns a Config with LavinMQ-compatible defaults.
func Default() *Config {
	return &Config{
		DataDir:                 DefaultDataDir,
		AMQPBind:                DefaultAMQPBind,
		AMQPPort:                DefaultAMQPPort,
		AMQPSPort:               DefaultAMQPSPort,
		MQTTBind:                DefaultMQTTBind,
		MQTTPort:                DefaultMQTTPort,
		HTTPBind:                DefaultHTTPBind,
		HTTPPort:                DefaultHTTPPort,
		Heartbeat:               DefaultHeartbeat,
		FrameMax:                DefaultFrameMax,
		ChannelMax:              DefaultChannelMax,
		MaxMessageSize:          DefaultMaxMessageSize,
		SegmentSize:             DefaultSegmentSize,
		SocketBufferSize:        DefaultSocketBufferSize,
		DefaultConsumerPrefetch: DefaultConsumerPrefetch,
		TCPNodelay:              false,
		TCPKeepalive:            true,
		TCPKeepaliveIdle:        DefaultTCPKeepaliveIdle,
		TCPKeepaliveInterval:    DefaultTCPKeepaliveInterval,
		TCPKeepaliveCount:       DefaultTCPKeepaliveCount,
		FreeDiskMin:             0,
		SetTimestamp:            false,
		DefaultUser:             DefaultUser,
		DefaultPasswordHash:     DefaultPasswordHash,
	}
}
