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
