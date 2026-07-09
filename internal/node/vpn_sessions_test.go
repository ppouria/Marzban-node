package node

import (
	"path/filepath"
	"testing"
)

func TestVPNSessionLedgerSharesLimitAcrossProtocols(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vpn-sessions.tsv")
	if !vpnAdmitGoSession(path, nil, vpnSessionEvent{
		UserID:     42,
		Protocol:   "wg",
		InboundTag: "wg-main",
		SessionID:  "wg:one",
		AssignedIP: "10.70.0.2",
		Event:      "start",
	}, 1) {
		t.Fatal("expected first WireGuard session to be admitted")
	}
	if vpnAdmitGoSession(path, nil, vpnSessionEvent{
		UserID:     42,
		Protocol:   "ov",
		InboundTag: "ov-main",
		SessionID:  "ov:two",
		AssignedIP: "10.66.0.2",
		Event:      "start",
	}, 1) {
		t.Fatal("expected OpenVPN session to be rejected by the shared limit")
	}
	if vpnUserCanOpenSession(path, 42, 1) {
		t.Fatal("expected user capacity to be full")
	}
	vpnReleaseGoSession(path, nil, vpnSessionEvent{
		UserID:    42,
		Protocol:  "wg",
		SessionID: "wg:one",
		Event:     "stop",
	})
	if !vpnUserCanOpenSession(path, 42, 1) {
		t.Fatal("expected user capacity after release")
	}
	if !vpnAdmitGoSession(path, nil, vpnSessionEvent{
		UserID:     42,
		Protocol:   "ov",
		InboundTag: "ov-main",
		SessionID:  "ov:two",
		AssignedIP: "10.66.0.2",
		Event:      "start",
	}, 1) {
		t.Fatal("expected OpenVPN session to be admitted after release")
	}
}
