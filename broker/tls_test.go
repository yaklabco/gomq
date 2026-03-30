package broker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yaklabco/gomq/amqp"
	"github.com/yaklabco/gomq/config"
)

func TestServer_ServeTLS(t *testing.T) {
	t.Parallel()

	certFile, keyFile := generateSelfSignedCert(t)

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Heartbeat = 0
	cfg.TLSCertFile = certFile
	cfg.TLSKeyFile = keyFile

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}

	tlsLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.ServeTLS(ctx, tlsLn)
	}()

	// Connect over TLS.
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // self-signed cert for testing
	}
	conn, err := tls.Dial("tcp", tlsLn.Addr().String(), tlsConfig)
	if err != nil {
		t.Fatalf("tls.Dial() error: %v", err)
	}
	defer conn.Close()

	// Send AMQP protocol header.
	if _, err := conn.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}); err != nil {
		t.Fatalf("write protocol header: %v", err)
	}

	// Read Connection.Start to verify the TLS connection works.
	reader := amqp.NewReader(conn, config.DefaultFrameMax)
	frame, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}

	mf, ok := frame.(*amqp.MethodFrame)
	if !ok {
		t.Fatalf("expected MethodFrame, got %T", frame)
	}

	if _, ok := mf.Method.(*amqp.ConnectionStart); !ok {
		t.Errorf("expected ConnectionStart, got %s", mf.Method.MethodName())
	}

	cancel()
	if err := srv.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestServer_TLSConfig(t *testing.T) {
	t.Parallel()

	certFile, keyFile := generateSelfSignedCert(t)

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.TLSCertFile = certFile
	cfg.TLSKeyFile = keyFile

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}

	tlsCfg, err := srv.TLSConfig()
	if err != nil {
		t.Fatalf("TLSConfig() error: %v", err)
	}

	if len(tlsCfg.Certificates) != 1 {
		t.Errorf("TLSConfig().Certificates length = %d, want 1", len(tlsCfg.Certificates))
	}

	if tlsCfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("TLSConfig().MinVersion = %d, want %d", tlsCfg.MinVersion, tls.VersionTLS12)
	}
}

func TestServer_TLSConfig_MissingCert(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.TLSCertFile = "/nonexistent/cert.pem"
	cfg.TLSKeyFile = "/nonexistent/key.pem"

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}

	_, err = srv.TLSConfig()
	if err == nil {
		t.Error("TLSConfig() expected error for missing cert file")
	}
}

func TestServer_TLSConfig_NoCertConfigured(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.DataDir = t.TempDir()

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}

	_, err = srv.TLSConfig()
	if err == nil {
		t.Error("TLSConfig() expected error when no TLS configured")
	}
}

// generateSelfSignedCert creates a self-signed certificate and key pair
// in the test's temporary directory.
func generateSelfSignedCert(t *testing.T) (string, string) { //nolint:unparam // returns cert and key file paths
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
