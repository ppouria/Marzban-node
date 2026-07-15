package node

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestVPNSessionLedgerHonorsMasterAdmission(t *testing.T) {
	status := http.StatusConflict
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "vpn-sessions.tsv")
	callback := &vpnSessionCallback{URL: server.URL, Token: "token", NodeID: 7}
	event := vpnSessionEvent{UserID: 42, Protocol: "ov", InboundTag: "ov-main", SessionID: "ov:one", ClientIP: "198.51.100.10", Event: "start"}
	if vpnAdmitGoSession(path, callback, event, 1) {
		t.Fatal("expected master to reject the session")
	}
	if records := vpnSessionRecordsLocked(path); len(records) != 0 {
		t.Fatalf("rejected session was persisted: %#v", records)
	}

	status = http.StatusOK
	if !vpnAdmitGoSession(path, callback, event, 1) {
		t.Fatal("expected master-approved session to be admitted")
	}
}

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

func TestVPNSessionLedgerRejectsAssignedIPConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vpn-sessions.tsv")
	if !vpnAdmitGoSession(path, nil, vpnSessionEvent{
		UserID:     42,
		Protocol:   "ov",
		InboundTag: "ov-main",
		SessionID:  "ov:one",
		AssignedIP: "10.66.0.2",
		Event:      "start",
	}, 0) {
		t.Fatal("expected first OpenVPN session to be admitted")
	}
	if vpnAdmitGoSession(path, nil, vpnSessionEvent{
		UserID:     43,
		Protocol:   "ov",
		InboundTag: "ov-main",
		SessionID:  "ov:two",
		AssignedIP: "10.66.0.2",
		Event:      "start",
	}, 0) {
		t.Fatal("expected duplicate assigned IP to be rejected")
	}
}
