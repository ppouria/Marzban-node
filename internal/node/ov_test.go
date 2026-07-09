package node

import (
	"strings"
	"testing"
)

func TestOVServerConfigUsesTCPServerProto(t *testing.T) {
	config := serverConfig(ovRuntimeInbound{
		Tag:       "ov-tcp",
		Port:      1194,
		Transport: "tcp",
		Settings:  map[string]any{"transport": "tcp"},
	}, "/tmp/ov", "/tmp/ov/ccd")

	if !strings.Contains(config, "proto tcp-server\n") {
		t.Fatalf("server config does not use tcp-server:\n%s", config)
	}
	if strings.Contains(config, "fast-io\n") {
		t.Fatalf("tcp server config should not enable fast-io:\n%s", config)
	}
}

func TestOVServerConfigEnablesFastIOForUDP(t *testing.T) {
	config := serverConfig(ovRuntimeInbound{
		Tag:       "ov-udp",
		Port:      1194,
		Transport: "udp",
		Settings:  map[string]any{"transport": "udp"},
	}, "/tmp/ov", "/tmp/ov/ccd")

	if !strings.Contains(config, "proto udp\nfast-io\n") {
		t.Fatalf("udp server config should enable fast-io:\n%s", config)
	}
}

func TestOVServerConfigRequiresDCODataCiphers(t *testing.T) {
	config := serverConfig(ovRuntimeInbound{
		Tag:       "ov-dco",
		Port:      1194,
		Transport: "udp",
		Settings: map[string]any{
			"transport":   "udp",
			"require_dco": true,
			"cipher":      "AES-256-GCM",
		},
	}, "/tmp/ov", "/tmp/ov/ccd")

	for _, want := range []string{
		"proto udp\n",
		"fast-io\n",
		"cipher AES-256-GCM\n",
		"data-ciphers " + ovDCODataCiphers + "\n",
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("server config missing %q:\n%s", want, config)
		}
	}
}

func TestOVDCORejectsLegacyCipher(t *testing.T) {
	err := validateOVDCOSettings(ovRuntimeInbound{
		Tag:      "ov",
		Settings: map[string]any{"require_dco": true, "cipher": "AES-256-CBC"},
	})
	if err == nil || !strings.Contains(err.Error(), "not DCO-compatible") {
		t.Fatalf("expected DCO cipher error, got %v", err)
	}
}

func TestOVDCOInactiveReasonDetectsFallback(t *testing.T) {
	reason := ovDCOInactiveReason("Kernel support for ovpn-dco missing, disabling data channel offload.")
	if reason == "" {
		t.Fatal("expected inactive DCO reason")
	}
}
