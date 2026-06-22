package xray

import (
	"net"
	"strings"
	"testing"
)

func TestOutboundTargetFromConfigVLESS(t *testing.T) {
	target, ok := outboundTargetFromConfig(map[string]any{
		"tag":      "proxy",
		"protocol": "vless",
		"settings": map[string]any{
			"vnext": []any{
				map[string]any{"address": "1.1.1.1", "port": float64(443)},
			},
		},
	})
	if !ok || target.Address != "1.1.1.1" || target.Port != 443 {
		t.Fatalf("unexpected target ok=%v target=%#v", ok, target)
	}
}

func TestOutboundTargetFromConfigWireguardEndpoint(t *testing.T) {
	target, ok := outboundTargetFromConfig(map[string]any{
		"tag":      "wg",
		"protocol": "wireguard",
		"settings": map[string]any{
			"peers": []any{
				map[string]any{"endpoint": "engage.cloudflareclient.com:2408"},
			},
		},
	})
	if !ok || target.Address != "engage.cloudflareclient.com" || target.Port != 2408 {
		t.Fatalf("unexpected target ok=%v target=%#v", ok, target)
	}
}

func TestParsePingDelay(t *testing.T) {
	output := "64 bytes from 1.1.1.1: icmp_seq=1 ttl=57 time=12.7 ms"
	if got := parsePingDelay(output); got != 13 {
		t.Fatalf("parsePingDelay()=%d, want 13", got)
	}
}

func TestTCPOutboundTestConnectsToTarget(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()

	addr := listener.Addr().(*net.TCPAddr)
	result := (&Core{}).TestOutbound("proxy", "vless", []map[string]any{
		{
			"tag":      "proxy",
			"protocol": "vless",
			"settings": map[string]any{
				"vnext": []any{map[string]any{"address": "127.0.0.1", "port": addr.Port}},
			},
		},
	}, "", "tcp")
	if !result.Success || result.TestType != "tcp" || result.Address != "127.0.0.1" || result.Port != addr.Port {
		t.Fatalf("unexpected tcp result: %#v", result)
	}
}

func TestICMPOutboundTestRequiresAddress(t *testing.T) {
	result := (&Core{}).TestOutbound("proxy", "vless", []map[string]any{
		{"tag": "proxy", "protocol": "vless", "settings": map[string]any{}},
	}, "", "icmp")
	if result.Success || result.TestType != "icmp" || !strings.Contains(result.Error, "address") {
		t.Fatalf("unexpected icmp result: %#v", result)
	}
}
