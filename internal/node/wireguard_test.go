package node

import (
	"strings"
	"testing"
)

func TestWGValidateInboundsRejectsPeerOutsidePool(t *testing.T) {
	err := wgValidateInbounds([]wgRuntimeInbound{{
		Tag:        "wg",
		ListenPort: 51820,
		Settings:   map[string]any{"address_pool": "10.70.0.0/24"},
		Peers:      []wgRuntimePeer{{UserID: 7, Address: "10.71.0.2"}},
	}})
	if err == nil || !strings.Contains(err.Error(), "outside pool") {
		t.Fatalf("expected outside pool error, got %v", err)
	}
}

func TestWGValidateInboundsRejectsPeerServerAddress(t *testing.T) {
	err := wgValidateInbounds([]wgRuntimeInbound{{
		Tag:        "wg",
		ListenPort: 51820,
		Settings:   map[string]any{"address_pool": "10.70.0.0/24"},
		Peers:      []wgRuntimePeer{{UserID: 7, Address: "10.70.0.1"}},
	}})
	if err == nil || !strings.Contains(err.Error(), "server address") {
		t.Fatalf("expected server address error, got %v", err)
	}
}

func TestWGValidateInboundsRejectsDuplicatePeerAddress(t *testing.T) {
	err := wgValidateInbounds([]wgRuntimeInbound{{
		Tag:        "wg",
		ListenPort: 51820,
		Settings:   map[string]any{"address_pool": "10.70.0.0/24"},
		Peers: []wgRuntimePeer{
			{UserID: 7, Address: "10.70.0.2"},
			{UserID: 8, Address: "10.70.0.2/32"},
		},
	}})
	if err == nil || !strings.Contains(err.Error(), "share address") {
		t.Fatalf("expected duplicate address error, got %v", err)
	}
}

func TestWGTProxyScriptRoutesToTunnelPort(t *testing.T) {
	script := wgTProxyScript(wgRuntimeInbound{Tag: "wg", TunnelPort: 17020}, "rbwg12345678")
	for _, want := range []string{
		"table inet rebecca_wireguard_rbwg12345678",
		`iifname "rbwg12345678"`,
		"tproxy ip to 127.0.0.1:17020",
		"meta mark set 1 accept",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q:\n%s", want, script)
		}
	}
}
