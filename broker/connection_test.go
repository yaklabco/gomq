package broker

import (
	"net"
	"testing"
	"time"

	"github.com/jamesainslie/gomq/amqp"
	"github.com/jamesainslie/gomq/auth"
	"github.com/jamesainslie/gomq/config"
)

// testSetup creates a temp data dir with a default vhost and user store.
func testSetup(t *testing.T) (*VHost, *auth.UserStore) {
	t.Helper()
	dataDir := t.TempDir()

	vh, err := NewVHost("/", dataDir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	t.Cleanup(func() { _ = vh.Close() })

	users, err := auth.NewUserStore(dataDir)
	if err != nil {
		t.Fatalf("NewUserStore() error: %v", err)
	}

	return vh, users
}

// doClientHandshake performs the client side of the AMQP handshake on conn
// using the default "/" vhost.
func doClientHandshake(t *testing.T, conn net.Conn) {
	t.Helper()

	// Send protocol header.
	header := []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}
	if _, err := conn.Write(header); err != nil {
		t.Fatalf("write protocol header: %v", err)
	}

	reader := amqp.NewReader(conn, config.DefaultFrameMax)
	writer := amqp.NewWriter(conn, int(config.DefaultFrameMax))

	// Read Connection.Start.
	frame, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("read Connection.Start frame: %v", err)
	}
	mf, ok := frame.(*amqp.MethodFrame)
	if !ok {
		t.Fatalf("expected MethodFrame, got %T", frame)
	}
	if _, isStart := mf.Method.(*amqp.ConnectionStart); !isStart {
		t.Fatalf("expected ConnectionStart, got %s", mf.Method.MethodName())
	}

	// Send Connection.StartOk with PLAIN credentials.
	response := []byte("\x00guest\x00guest")
	if err := writer.WriteMethod(0, &amqp.ConnectionStartOk{
		ClientProperties: amqp.Table{"product": "test-client"},
		Mechanism:        "PLAIN",
		Response:         response,
		Locale:           "en_US",
	}); err != nil {
		t.Fatalf("write StartOk: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush StartOk: %v", err)
	}

	// Read Connection.Tune.
	frame, err = reader.ReadFrame()
	if err != nil {
		t.Fatalf("read Connection.Tune: %v", err)
	}
	mf, ok = frame.(*amqp.MethodFrame)
	if !ok {
		t.Fatalf("expected MethodFrame, got %T", frame)
	}
	tune, ok := mf.Method.(*amqp.ConnectionTune)
	if !ok {
		t.Fatalf("expected ConnectionTune, got %s", mf.Method.MethodName())
	}

	// Send Connection.TuneOk.
	if err := writer.WriteMethod(0, &amqp.ConnectionTuneOk{
		ChannelMax: tune.ChannelMax,
		FrameMax:   tune.FrameMax,
		Heartbeat:  tune.Heartbeat,
	}); err != nil {
		t.Fatalf("write TuneOk: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush TuneOk: %v", err)
	}

	// Send Connection.Open.
	if err := writer.WriteMethod(0, &amqp.ConnectionOpen{
		VirtualHost: "/",
	}); err != nil {
		t.Fatalf("write ConnectionOpen: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush ConnectionOpen: %v", err)
	}

	// Read Connection.OpenOk.
	frame, err = reader.ReadFrame()
	if err != nil {
		t.Fatalf("read Connection.OpenOk: %v", err)
	}
	mf, ok = frame.(*amqp.MethodFrame)
	if !ok {
		t.Fatalf("expected MethodFrame, got %T", frame)
	}
	if _, ok := mf.Method.(*amqp.ConnectionOpenOk); !ok {
		t.Fatalf("expected ConnectionOpenOk, got %s", mf.Method.MethodName())
	}
}

func TestConnection_HandshakeSuccess(t *testing.T) {
	t.Parallel()
	vh, users := testSetup(t)
	cfg := config.Default()
	cfg.Heartbeat = 0 // disable heartbeat for test

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	vhosts := map[string]*VHost{"/": vh}
	conn := NewConnection(serverConn, cfg, vhosts, users)

	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.Handshake()
	}()

	// Set a deadline on the client side for the handshake.
	if err := clientConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	doClientHandshake(t, clientConn)

	if err := <-errCh; err != nil {
		t.Fatalf("Handshake() error: %v", err)
	}

	if conn.vhost != vh {
		t.Error("connection vhost not set after handshake")
	}
}

func TestConnection_BadProtocolHeader(t *testing.T) {
	t.Parallel()
	vh, users := testSetup(t)
	cfg := config.Default()

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	vhosts := map[string]*VHost{"/": vh}
	conn := NewConnection(serverConn, cfg, vhosts, users)

	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.Handshake()
	}()

	// Send invalid header.
	if _, err := clientConn.Write([]byte("HTTP/1.1")); err != nil {
		t.Fatalf("write bad header: %v", err)
	}

	err := <-errCh
	if err == nil {
		t.Fatal("expected error for bad protocol header")
	}
}

func TestConnection_WrongCredentials(t *testing.T) {
	t.Parallel()
	vh, users := testSetup(t)
	cfg := config.Default()

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	vhosts := map[string]*VHost{"/": vh}
	conn := NewConnection(serverConn, cfg, vhosts, users)

	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.Handshake()
	}()

	// Send protocol header.
	header := []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}
	if _, err := clientConn.Write(header); err != nil {
		t.Fatalf("write header: %v", err)
	}

	reader := amqp.NewReader(clientConn, config.DefaultFrameMax)
	writer := amqp.NewWriter(clientConn, int(config.DefaultFrameMax))

	// Read Connection.Start.
	if _, err := reader.ReadFrame(); err != nil {
		t.Fatalf("read Start: %v", err)
	}

	// Send wrong password.
	if err := writer.WriteMethod(0, &amqp.ConnectionStartOk{
		Mechanism: "PLAIN",
		Response:  []byte("\x00guest\x00wrongpassword"),
		Locale:    "en_US",
	}); err != nil {
		t.Fatalf("write StartOk: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush StartOk: %v", err)
	}

	err := <-errCh
	if err == nil {
		t.Fatal("expected error for wrong credentials")
	}
}

func TestConnection_ChannelOpenClose(t *testing.T) {
	t.Parallel()
	vh, users := testSetup(t)
	cfg := config.Default()
	cfg.Heartbeat = 0

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	vhosts := map[string]*VHost{"/": vh}
	conn := NewConnection(serverConn, cfg, vhosts, users)

	// Handshake in background.
	handshakeErr := make(chan error, 1)
	go func() {
		handshakeErr <- conn.Handshake()
	}()

	if err := clientConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	doClientHandshake(t, clientConn)

	if err := <-handshakeErr; err != nil {
		t.Fatalf("Handshake() error: %v", err)
	}

	// Clear deadline for the rest of the test.
	if err := clientConn.SetDeadline(time.Time{}); err != nil {
		t.Fatalf("clear deadline: %v", err)
	}

	// Start read loop in background.
	readLoopErr := make(chan error, 1)
	go func() {
		readLoopErr <- conn.ReadLoop()
	}()

	reader := amqp.NewReader(clientConn, config.DefaultFrameMax)
	writer := amqp.NewWriter(clientConn, int(config.DefaultFrameMax))

	// Open channel 1.
	if err := writer.WriteMethod(1, &amqp.ChannelOpen{}); err != nil {
		t.Fatalf("write ChannelOpen: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush ChannelOpen: %v", err)
	}

	// Read ChannelOpenOk.
	expectFrameMethod(t, reader, "ChannelOpenOk", func(method amqp.Method) bool {
		_, match := method.(*amqp.ChannelOpenOk)
		return match
	})

	// Close channel 1.
	if err := writer.WriteMethod(1, &amqp.ChannelClose{ReplyCode: amqp.ReplySuccess, ReplyText: "normal"}); err != nil {
		t.Fatalf("write ChannelClose: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush ChannelClose: %v", err)
	}

	// Read ChannelCloseOk.
	expectFrameMethod(t, reader, "ChannelCloseOk", func(method amqp.Method) bool {
		_, match := method.(*amqp.ChannelCloseOk)
		return match
	})

	// Close connection.
	if err := writer.WriteMethod(0, &amqp.ConnectionClose{ReplyCode: amqp.ReplySuccess, ReplyText: "bye"}); err != nil {
		t.Fatalf("write ConnectionClose: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush ConnectionClose: %v", err)
	}

	// Read ConnectionCloseOk.
	expectFrameMethod(t, reader, "ConnectionCloseOk", func(method amqp.Method) bool {
		_, match := method.(*amqp.ConnectionCloseOk)
		return match
	})

	// Wait for read loop to finish.
	select {
	case err := <-readLoopErr:
		if err != nil {
			t.Fatalf("ReadLoop() error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadLoop() did not finish")
	}
}

// expectFrameMethod reads a frame and asserts it contains the expected method type.
func expectFrameMethod(t *testing.T, reader *amqp.Reader, name string, match func(amqp.Method) bool) {
	t.Helper()
	frame, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	mf, ok := frame.(*amqp.MethodFrame)
	if !ok {
		t.Fatalf("expected MethodFrame for %s, got %T", name, frame)
	}
	if !match(mf.Method) {
		t.Fatalf("expected %s, got %s", name, mf.Method.MethodName())
	}
}
