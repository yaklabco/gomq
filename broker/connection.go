package broker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jamesainslie/gomq/amqp"
	"github.com/jamesainslie/gomq/auth"
	"github.com/jamesainslie/gomq/config"
)

// protocolHeader is the AMQP 0-9-1 protocol header sent by clients.
//
//nolint:gochecknoglobals // read-only protocol constant
var protocolHeader = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

// heartbeatMultiplier is how many heartbeat intervals without frames before timeout.
const heartbeatMultiplier = 2

// amqpVersionMinor is the AMQP 0-9-1 minor version number.
const amqpVersionMinor = 9

// minPlainResponseLen is the minimum length of a SASL PLAIN response (\x00u\x00p).
const minPlainResponseLen = 3

// Connection manages an AMQP client connection including handshake,
// frame reading, channel dispatch, and heartbeat monitoring.
type Connection struct {
	mu        sync.Mutex
	conn      net.Conn
	reader    *amqp.Reader
	writer    *amqp.Writer
	vhost     *VHost
	user      *auth.User
	channels  map[uint16]*Channel
	heartbeat time.Duration
	frameMax  uint32
	closed    atomic.Bool
	cfg       *config.Config
	vhosts    map[string]*VHost
	users     *auth.UserStore
}

// NewConnection wraps a net.Conn as an AMQP connection ready for handshake.
func NewConnection(
	conn net.Conn,
	cfg *config.Config,
	vhosts map[string]*VHost,
	users *auth.UserStore,
) *Connection {
	return &Connection{
		conn:     conn,
		reader:   amqp.NewReader(conn, cfg.FrameMax),
		writer:   amqp.NewWriter(conn, int(cfg.FrameMax)),
		channels: make(map[uint16]*Channel),
		frameMax: cfg.FrameMax,
		cfg:      cfg,
		vhosts:   vhosts,
		users:    users,
	}
}

// Handshake performs the AMQP connection handshake: protocol header,
// Start/StartOk, Tune/TuneOk, Open/OpenOk.
func (c *Connection) Handshake() error {
	// Read protocol header (8 bytes).
	var header [8]byte
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return fmt.Errorf("read protocol header: %w", err)
	}
	if !bytes.Equal(header[:], protocolHeader) {
		return fmt.Errorf("invalid protocol header: %x", header)
	}

	// Send Connection.Start.
	if err := c.sendFrame(0, &amqp.ConnectionStart{
		VersionMajor: 0,
		VersionMinor: amqpVersionMinor,
		ServerProperties: amqp.Table{
			"product": "gomq",
			"version": "0.1.0",
			"capabilities": amqp.Table{
				"publisher_confirms":     true,
				"basic.nack":             true,
				"consumer_cancel_notify": true,
				"connection.blocked":     true,
				"per_consumer_qos":       true,
				"direct_reply_to":        true,
			},
		},
		Mechanisms: []byte("PLAIN"),
		Locales:    []byte("en_US"),
	}); err != nil {
		return fmt.Errorf("send Connection.Start: %w", err)
	}

	// Read Connection.StartOk.
	frame, err := c.reader.ReadFrame()
	if err != nil {
		return fmt.Errorf("read Connection.StartOk: %w", err)
	}
	startOk, err := expectMethod[*amqp.ConnectionStartOk](frame)
	if err != nil {
		return err
	}

	// Authenticate via PLAIN: \x00username\x00password.
	username, password, parseErr := parsePlainCredentials(startOk.Response)
	if parseErr != nil {
		return fmt.Errorf("parse PLAIN credentials: %w", parseErr)
	}

	user, authErr := c.users.Authenticate(username, password)
	if authErr != nil {
		return fmt.Errorf("authentication failed: %w", authErr)
	}
	c.user = user

	// Send Connection.Tune.
	heartbeatSec := uint16(c.cfg.Heartbeat.Seconds())
	if err := c.sendFrame(0, &amqp.ConnectionTune{
		ChannelMax: c.cfg.ChannelMax,
		FrameMax:   c.cfg.FrameMax,
		Heartbeat:  heartbeatSec,
	}); err != nil {
		return fmt.Errorf("send Connection.Tune: %w", err)
	}

	// Read Connection.TuneOk.
	frame, err = c.reader.ReadFrame()
	if err != nil {
		return fmt.Errorf("read Connection.TuneOk: %w", err)
	}
	tuneOk, err := expectMethod[*amqp.ConnectionTuneOk](frame)
	if err != nil {
		return err
	}

	// Negotiate lower values.
	if tuneOk.FrameMax > 0 && tuneOk.FrameMax < c.frameMax {
		c.frameMax = tuneOk.FrameMax
	}
	if tuneOk.Heartbeat > 0 && tuneOk.Heartbeat < heartbeatSec {
		heartbeatSec = tuneOk.Heartbeat
	}
	c.heartbeat = time.Duration(heartbeatSec) * time.Second

	// Read Connection.Open.
	frame, err = c.reader.ReadFrame()
	if err != nil {
		return fmt.Errorf("read Connection.Open: %w", err)
	}
	open, err := expectMethod[*amqp.ConnectionOpen](frame)
	if err != nil {
		return err
	}

	// Look up vhost and check permission.
	vh, ok := c.vhosts[open.VirtualHost]
	if !ok {
		return fmt.Errorf("vhost %q: %w", open.VirtualHost, ErrVHostNotFound)
	}
	if !c.user.CheckPermission(open.VirtualHost, "", "read") {
		return fmt.Errorf("access refused to vhost %q", open.VirtualHost)
	}
	c.vhost = vh

	// Send Connection.OpenOk.
	if err := c.sendFrame(0, &amqp.ConnectionOpenOk{}); err != nil {
		return fmt.Errorf("send Connection.OpenOk: %w", err)
	}

	return nil
}

// ReadLoop reads frames from the connection and dispatches them.
// It returns nil on graceful close and an error on protocol or I/O errors.
//
// Instead of calling SetReadDeadline on every frame (one syscall per frame),
// a separate goroutine monitors a channel signal to detect heartbeat timeouts.
func (c *Connection) ReadLoop() error {
	lastFrame := make(chan struct{}, 1)
	done := make(chan struct{})
	defer close(done)

	// Heartbeat timeout goroutine — closes the connection if no frames
	// arrive within the heartbeat window, which unblocks ReadFrame below.
	// Exits when done is closed (ReadLoop returns).
	if c.heartbeat > 0 {
		go func() {
			timeout := c.heartbeat * heartbeatMultiplier
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			for {
				select {
				case <-done:
					return
				case <-lastFrame:
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(timeout)
				case <-timer.C:
					_ = c.conn.Close() // triggers ReadFrame error
					return
				}
			}
		}()
	}

	for {
		frame, err := c.reader.ReadFrame()
		if err != nil {
			if c.closed.Load() {
				return nil
			}
			return fmt.Errorf("read frame: %w", err)
		}

		// Signal heartbeat goroutine (non-blocking).
		select {
		case lastFrame <- struct{}{}:
		default:
		}

		switch frame := frame.(type) {
		case *amqp.HeartbeatFrame:
			if sendErr := c.sendHeartbeat(); sendErr != nil {
				return fmt.Errorf("send heartbeat: %w", sendErr)
			}

		case *amqp.MethodFrame:
			if err := c.dispatchMethod(frame); err != nil {
				return err
			}

		case *amqp.HeaderFrame:
			channel, ok := c.getChannel(frame.Channel)
			if !ok {
				return fmt.Errorf("header frame for unknown channel %d", frame.Channel)
			}
			if err := channel.HandleHeader(frame); err != nil {
				return fmt.Errorf("channel %d handle header: %w", frame.Channel, err)
			}

		case *amqp.BodyFrame:
			channel, ok := c.getChannel(frame.Channel)
			if !ok {
				return fmt.Errorf("body frame for unknown channel %d", frame.Channel)
			}
			if err := channel.HandleBody(frame); err != nil {
				return fmt.Errorf("channel %d handle body: %w", frame.Channel, err)
			}
		}
	}
}

// Name returns a unique identifier for this connection.
func (c *Connection) Name() string {
	return c.conn.RemoteAddr().String()
}

// VHostName returns the name of the vhost this connection is using.
// Returns an empty string if the handshake has not completed.
func (c *Connection) VHostName() string {
	if c.vhost == nil {
		return ""
	}
	return c.vhost.Name()
}

// UserName returns the authenticated username.
// Returns an empty string if the handshake has not completed.
func (c *Connection) UserName() string {
	if c.user == nil {
		return ""
	}
	return c.user.Name
}

// ChannelCount returns the number of open channels.
func (c *Connection) ChannelCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.channels)
}

// Channels returns a snapshot of all open channels.
func (c *Connection) Channels() []*Channel {
	c.mu.Lock()
	defer c.mu.Unlock()

	chs := make([]*Channel, 0, len(c.channels))
	for _, ch := range c.channels {
		chs = append(chs, ch)
	}

	return chs
}

// Close gracefully shuts down the connection.
func (c *Connection) Close() {
	if c.closed.Swap(true) {
		return
	}

	c.mu.Lock()
	for _, channel := range c.channels {
		channel.Close()
	}
	clear(c.channels)
	c.mu.Unlock()

	_ = c.conn.Close() // safe: triggers read loop exit
}

// --- Frame dispatch ---

func (c *Connection) dispatchMethod(frame *amqp.MethodFrame) error {
	// Channel 0: connection-level methods.
	if frame.Channel == 0 {
		return c.handleConnectionMethod(frame.Method)
	}

	switch method := frame.Method.(type) {
	case *amqp.ChannelOpen:
		return c.handleChannelOpen(frame.Channel, method)
	case *amqp.ChannelClose:
		return c.handleChannelClose(frame.Channel, method)
	default:
		channel, ok := c.getChannel(frame.Channel)
		if !ok {
			return fmt.Errorf("method for unknown channel %d", frame.Channel)
		}
		return channel.HandleMethod(frame.Method)
	}
}

func (c *Connection) handleConnectionMethod(method amqp.Method) error {
	switch method.(type) {
	case *amqp.ConnectionClose:
		if err := c.sendFrame(0, &amqp.ConnectionCloseOk{}); err != nil {
			return fmt.Errorf("send ConnectionCloseOk: %w", err)
		}
		c.Close()
		return nil
	default:
		return fmt.Errorf("unexpected connection method: %s", method.MethodName())
	}
}

func (c *Connection) handleChannelOpen(channelID uint16, _ *amqp.ChannelOpen) error {
	c.mu.Lock()

	if _, exists := c.channels[channelID]; exists {
		c.mu.Unlock()
		return fmt.Errorf("channel %d already open", channelID)
	}

	channel := NewChannel(channelID, c.vhost, c.sendFrameMethod, c.sendFrameContent, c.sendDelivery)
	c.channels[channelID] = channel
	c.mu.Unlock()

	return c.sendFrame(channelID, &amqp.ChannelOpenOk{})
}

func (c *Connection) handleChannelClose(channelID uint16, _ *amqp.ChannelClose) error {
	c.mu.Lock()
	channel, ok := c.channels[channelID]
	if ok {
		delete(c.channels, channelID)
	}
	c.mu.Unlock()

	if ok {
		channel.Close()
	}

	return c.sendFrame(channelID, &amqp.ChannelCloseOk{})
}

// --- Channel lookup ---

func (c *Connection) getChannel(channelID uint16) (*Channel, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	channel, ok := c.channels[channelID]
	return channel, ok
}

// --- Write helpers ---

// sendFrame writes a method frame to the connection, thread-safe.
func (c *Connection) sendFrame(channel uint16, method amqp.Method) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.writer.WriteMethod(channel, method); err != nil {
		return fmt.Errorf("write method %s: %w", method.MethodName(), err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("flush after %s: %w", method.MethodName(), err)
	}
	return nil
}

// sendFrameMethod is the sendMethodFn callback for channels.
func (c *Connection) sendFrameMethod(channel uint16, method amqp.Method) error {
	return c.sendFrame(channel, method)
}

// sendFrameContent writes a content header and body frames for a channel.
func (c *Connection) sendFrameContent(channel uint16, classID uint16, props *amqp.Properties, body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.writer.WriteHeader(channel, classID, uint64(len(body)), props); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if err := c.writer.WriteBody(channel, body); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("flush content: %w", err)
	}
	return nil
}

// sendDelivery writes a method frame, content header, and body in a single
// flush to reduce syscalls on the delivery hot path.
func (c *Connection) sendDelivery(channel uint16, method amqp.Method, classID uint16, props *amqp.Properties, body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Cork the TCP socket to batch method + header + body into fewer TCP
	// segments. On non-Linux platforms this is a no-op.
	setCork(c.conn, true)
	defer setCork(c.conn, false)

	deliver, ok := method.(*amqp.BasicDeliver)
	if ok {
		// Fast path: use coalesced delivery writer.
		return c.writer.WriteDelivery(channel, deliver, classID, uint64(len(body)), props, body)
	}

	// Fallback for non-deliver methods.
	if err := c.writer.WriteMethod(channel, method); err != nil {
		return fmt.Errorf("write delivery method %s: %w", method.MethodName(), err)
	}
	if err := c.writer.WriteHeader(channel, classID, uint64(len(body)), props); err != nil {
		return fmt.Errorf("write delivery header: %w", err)
	}
	if err := c.writer.WriteBody(channel, body); err != nil {
		return fmt.Errorf("write delivery body: %w", err)
	}
	return c.writer.Flush()
}

// sendHeartbeat writes a heartbeat frame.
func (c *Connection) sendHeartbeat() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.writer.WriteHeartbeat(); err != nil {
		return fmt.Errorf("write heartbeat: %w", err)
	}
	return c.writer.Flush()
}

// SendBlocked sends a Connection.Blocked frame to the client, indicating
// the broker has hit a resource limit (e.g. low disk space).
func (c *Connection) SendBlocked(reason string) error {
	return c.sendFrame(0, &amqp.ConnectionBlocked{Reason: reason})
}

// SendUnblocked sends a Connection.Unblocked frame to the client, indicating
// the resource constraint has been resolved.
func (c *Connection) SendUnblocked() error {
	return c.sendFrame(0, &amqp.ConnectionUnblocked{})
}

// --- PLAIN credential parsing ---

// parsePlainCredentials parses SASL PLAIN response: \x00username\x00password.
func parsePlainCredentials(response []byte) (string, string, error) {
	if len(response) < minPlainResponseLen || response[0] != 0 {
		return "", "", fmt.Errorf("malformed PLAIN response (length=%d)", len(response))
	}

	// Find the second null byte separating username from password.
	idx := bytes.IndexByte(response[1:], 0)
	if idx < 0 {
		return "", "", errors.New("malformed PLAIN response: missing password separator")
	}

	username := string(response[1 : 1+idx])
	password := string(response[2+idx:])

	return username, password, nil
}

// --- Helper generics ---

// expectMethod extracts a typed method from a frame, returning an error if the
// frame is not a MethodFrame or does not contain the expected method type.
func expectMethod[T amqp.Method](frame amqp.Frame) (T, error) {
	var zero T
	mf, ok := frame.(*amqp.MethodFrame)
	if !ok {
		return zero, fmt.Errorf("expected MethodFrame, got %T", frame)
	}
	typed, ok := mf.Method.(T)
	if !ok {
		return zero, fmt.Errorf("expected %T, got %s", zero, mf.Method.MethodName())
	}
	return typed, nil
}

// ErrVHostNotFound is returned when a requested vhost does not exist.
var ErrVHostNotFound = errors.New("vhost not found")
