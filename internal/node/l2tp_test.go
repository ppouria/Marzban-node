package node

import "testing"

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
