package node

import (
	"strings"
	"testing"
)

func TestL2TPPoolRangeUsesCIDRCapacity(t *testing.T) {
	local, pool := l2tpPoolRange("10.67.0.0/16")
	if local != "10.67.0.1" {
		t.Fatalf("unexpected local IP: %s", local)
	}
	if pool != "10.67.0.10-10.67.255.254" {
		t.Fatalf("unexpected pool range: %s", pool)
	}
}

func TestL2TPPoolRangeHandlesSmallCIDR(t *testing.T) {
	local, pool := l2tpPoolRange("10.67.0.0/30")
	if local != "10.67.0.1" {
		t.Fatalf("unexpected local IP: %s", local)
	}
	if pool != "10.67.0.2-10.67.0.2" {
		t.Fatalf("unexpected pool range: %s", pool)
	}
}

func TestNormalizeL2TPRuntimeInboundLocksPorts(t *testing.T) {
	inbound := normalizeL2TPRuntimeInbound(l2tpRuntimeInbound{
		Port:       3974,
		TunnelPort: 41941,
		Settings: map[string]any{
			"l2tp_port":        3974,
			"ipsec_ike_port":   1500,
			"ipsec_nat_port":   14500,
			"xray_tunnel_port": 41941,
			"tproxy_port":      41941,
		},
	})

	if inbound.Port != 1701 || inbound.TunnelPort != 1702 {
		t.Fatalf("unexpected ports: public=%d tunnel=%d", inbound.Port, inbound.TunnelPort)
	}
	if inbound.Settings["l2tp_port"] != 1701 || inbound.Settings["ipsec_ike_port"] != 500 || inbound.Settings["ipsec_nat_port"] != 4500 || inbound.Settings["tunnel_port"] != 1702 {
		t.Fatalf("fixed settings were not enforced: %#v", inbound.Settings)
	}
	if _, ok := inbound.Settings["xray_tunnel_port"]; ok {
		t.Fatalf("legacy xray_tunnel_port should be removed")
	}
	if _, ok := inbound.Settings["tproxy_port"]; ok {
		t.Fatalf("legacy tproxy_port should be removed")
	}
}

func TestL2TPLibreswanConfigSupportsNATClients(t *testing.T) {
	config := l2tpLibreswanConfig(l2tpFixedPort)
	for _, expected := range []string{
		"conn rebecca-l2tp-nat",
		"rightsubnet=vhost:%no,%priv",
		"also=rebecca-l2tp",
		"encapsulation=yes",
	} {
		if !strings.Contains(config, expected) {
			t.Fatalf("Libreswan config is missing %q:\n%s", expected, config)
		}
	}
}
