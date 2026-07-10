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

func TestVPNSessionLedgerCountsClientIPOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vpn-sessions.tsv")
	if !vpnAdmitGoSession(path, nil, vpnSessionEvent{
		UserID:     42,
		Protocol:   "wg",
		InboundTag: "wg-main",
		SessionID:  "wg:one",
		AssignedIP: "10.70.0.2",
		ClientIP:   "198.51.100.10",
		Event:      "start",
	}, 1) {
		t.Fatal("expected first session to be admitted")
	}
	if !vpnAdmitGoSession(path, nil, vpnSessionEvent{
		UserID:     42,
		Protocol:   "ov",
		InboundTag: "ov-main",
		SessionID:  "ov:two",
		AssignedIP: "10.66.0.2",
		ClientIP:   "198.51.100.10",
		Event:      "start",
	}, 1) {
		t.Fatal("expected same client IP to share the occupied slot")
	}
	if vpnAdmitGoSession(path, nil, vpnSessionEvent{
		UserID:     42,
		Protocol:   "wg",
		InboundTag: "wg-main",
		SessionID:  "wg:three",
		AssignedIP: "10.70.0.3",
		ClientIP:   "198.51.100.11",
		Event:      "start",
	}, 1) {
		t.Fatal("expected different client IP to be rejected")
	}
}
