package gomq_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamesainslie/gomq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestTLS_PublishConsume(t *testing.T) {
	t.Parallel()

	certFile, keyFile := generateTestCert(t)

	dir := t.TempDir()
	brk, err := gomq.New(
		gomq.WithDataDir(dir),
		gomq.WithHTTPPort(-1),
		gomq.WithMQTTPort(-1),
		gomq.WithTLS(certFile, keyFile),
		gomq.WithAMQPSPort(0), // random port
	)
	if err != nil {
		t.Fatalf("gomq.New() error: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- brk.Serve(ctx, ln)
	}()

	// Wait for the broker to be ready.
	addr := brk.WaitForAddr(ctx)
	if addr == nil {
		cancel()
		t.Fatal("broker did not start")
	}

	// Wait briefly for the AMQPS listener to start.
	var amqpsAddr net.Addr
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		amqpsAddr = brk.AMQPSAddr()
		if amqpsAddr != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if amqpsAddr == nil {
		cancel()
		t.Fatal("AMQPS listener did not start")
	}

	t.Cleanup(func() {
		if closeErr := brk.Close(); closeErr != nil {
			t.Errorf("broker close: %v", closeErr)
		}
		cancel()
		if srvErr := <-serveErr; srvErr != nil {
			t.Errorf("broker serve: %v", srvErr)
		}
	})

	// Connect over TLS using amqp091-go.
	tcpAddr, ok := amqpsAddr.(*net.TCPAddr)
	if !ok {
		t.Fatalf("AMQPSAddr() type = %T, want *net.TCPAddr", amqpsAddr)
	}

	url := fmt.Sprintf("amqps://guest:guest@127.0.0.1:%d/", tcpAddr.Port)
	conn, err := amqp.DialTLS(url, &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // self-signed cert for testing
	})
	if err != nil {
		t.Fatalf("amqp.DialTLS() error: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	queue, err := ch.QueueDeclare("tls-test-queue", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	body := "hello over TLS"
	err = ch.PublishWithContext(context.Background(), "", queue.Name, false, false, amqp.Publishing{
		Body: []byte(body),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs, err := ch.Consume(queue.Name, "tls-consumer", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != body {
			t.Errorf("message body = %q, want %q", string(msg.Body), body)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for message")
	}
}

func generateTestCert(t *testing.T) (string, string) { //nolint:unparam // returns cert and key file paths
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"GoMQ Test"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	return certPath, keyPath
}
