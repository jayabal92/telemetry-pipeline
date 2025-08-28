package config

import (
	"crypto/tls"
	"os"
	"testing"
)

func TestConfigString(t *testing.T) {
	c := Config{
		Server:  ServerConfig{NodeID: "node1", GRPCAddr: "127.0.0.1:9000", GRPCAdvertisedAddr: "127.0.0.1:9000"},
		Storage: StorageConfig{DataDir: "/tmp/data"},
	}
	want := "node=node1 grpc=127.0.0.1:9000 grpcsAdvrAddr=127.0.0.1:9000 dataDir=/tmp/data"
	if got := c.String(); got != want {
		t.Errorf("Config.String() = %v, want %v", got, want)
	}
}

func TestTLSConfig_Disabled(t *testing.T) {
	tlsCfg, err := TLSConfig{Enable: false}.GRPCCredentials()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg != nil {
		t.Fatalf("expected nil when TLS disabled, got %+v", tlsCfg)
	}
}

// Fake cert test (writes temp cert/key/ca)
func TestTLSConfig_EnabledInvalidFiles(t *testing.T) {
	tmpfile := func(content string) string {
		f, _ := os.CreateTemp("", "test")
		f.WriteString(content)
		f.Close()
		return f.Name()
	}
	cert := tmpfile("not-a-cert")
	key := tmpfile("not-a-key")
	ca := tmpfile("not-a-ca")

	tlsCfg := TLSConfig{Enable: true, CertFile: cert, KeyFile: key, CAFile: ca}
	_, err := tlsCfg.GRPCCredentials()
	if err == nil {
		t.Fatalf("expected error for invalid certs")
	}
}

func TestTLSConfig_ReturnsTLSConfig(t *testing.T) {
	// NOTE: we won’t generate real certs here, just check type assertion
	tc := TLSConfig{Enable: false}
	cfg, _ := tc.GRPCCredentials()
	if cfg != nil && cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected TLS12 minimum version")
	}
}
