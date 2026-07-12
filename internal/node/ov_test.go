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
	if strings.Contains(config, "disable-dco\n") {
		t.Fatalf("DCO-required config should not disable DCO:\n%s", config)
	}
}

func TestOVServerConfigOmitsDCOByDefault(t *testing.T) {
	config := serverConfig(ovRuntimeInbound{
		Tag:       "ov-no-dco",
		Port:      1194,
		Transport: "udp",
		Settings:  map[string]any{"transport": "udp"},
	}, "/tmp/ov", "/tmp/ov/ccd")

	if strings.Contains(config, "disable-dco\n") {
		t.Fatalf("server config should remain compatible with OpenVPN 2.5:\n%s", config)
	}
	if strings.Contains(config, "data-ciphers "+ovDCODataCiphers+"\n") {
		t.Fatalf("DCO-disabled config should not force DCO data ciphers:\n%s", config)
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

func TestOVTProxyMasqueradesICMP(t *testing.T) {
	script := nftScript(ovRuntimeInbound{
		Tag:        "ov",
		TunnelPort: 41940,
		Settings: map[string]any{
			"tproxy_enabled": true,
			"ipv4_pool_cidr": "10.66.0.0/16",
		},
	}, "rbov12345678")

	for _, want := range []string{
		"meta l4proto { tcp, udp } tproxy ip to 127.0.0.1:41940",
		"type nat hook postrouting priority srcnat",
		"ip saddr 10.66.0.0/16 oifname != \"rbov12345678\" meta l4proto icmp masquerade",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("nft script missing %q:\n%s", want, script)
		}
	}
}

func TestOVAuthCountsPendingUsage(t *testing.T) {
	script := authScript("/tmp/users.tsv", "/tmp/usage.tsv", "/tmp/callback.env", "/tmp/sessions.tsv", "ov")
	for _, want := range []string{
		"USAGE=\"/tmp/usage.tsv\"",
		"pending[id] += $2",
		"used = $5 + pending[$1]",
		"\"$USAGE\" \"$USERS\"",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("auth script missing %q:\n%s", want, script)
		}
	}
}
