package node

import (
	"strings"
	"testing"
)

func TestSourceIPBlockPortsFromConfig(t *testing.T) {
	raw := []byte(`{
		"inbounds": [
			{"tag":"API_INBOUND","protocol":"tunnel","port":62051},
			{"tag":"vmess-tcp","protocol":"vmess","port":443,"streamSettings":{"network":"ws"}},
			{"tag":"ss-udp","protocol":"shadowsocks","port":8443,"settings":{"network":"tcp,udp"}},
			{"tag":"ov-tunnel","protocol":"tunnel","port":40001}
		]
	}`)

	ports, err := sourceIPBlockPortsFromConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got := sourceIPBlockPortSet(ports.TCP); got != "443, 8443" {
		t.Fatalf("tcp ports = %q", got)
	}
	if got := sourceIPBlockPortSet(ports.UDP); got != "8443" {
		t.Fatalf("udp ports = %q", got)
	}
}

func TestBuildSourceIPBlockNFTScriptScopesToPorts(t *testing.T) {
	entries, err := normalizeSourceIPBlockEntries([]sourceIPBlockEntry{
		{IP: "198.51.100.10", TTLSeconds: 60},
		{IP: "2001:db8::10", TTLSeconds: 120},
		{IP: "127.0.0.1", TTLSeconds: 60},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	script := buildSourceIPBlockNFTScript(entries, sourceIPBlockPorts{TCP: []uint32{443}, UDP: []uint32{8443}})
	for _, want := range []string{
		"table inet rebecca_xray_limiter",
		"198.51.100.10 timeout 60s",
		"2001:db8::10 timeout 120s",
		"ip saddr @blocked4 tcp dport { 443 } drop",
		"ip6 saddr @blocked6 udp dport { 8443 } drop",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("nft script missing %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "127.0.0.1") {
		t.Fatalf("loopback address should not be blocked:\n%s", script)
	}
}
