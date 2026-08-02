package node

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	appconfig "github.com/rebeccapanel/rebecca-node/internal/config"
)

func TestNodeReadyUsesCachedSessionCallback(t *testing.T) {
	received := make(chan vpnSessionEvent, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event vpnSessionEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		received <- event
	}))
	defer server.Close()

	node := &Server{settings: appconfig.Settings{RebeccaDataDir: t.TempDir()}}
	node.saveConfigCache(`{"inbounds":[]}`, "127.0.0.1", &ovRuntime{SessionCallback: &vpnSessionCallback{URL: server.URL, Token: "token", NodeID: 7}}, nil, nil, nil)
	node.notifyMasterReady()
	event := <-received
	if event.NodeID != 7 || event.Event != "ready" {
		t.Fatalf("ready event = %#v", event)
	}
}

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

func TestVPNSessionLedgerReplacesOpenVPNReconnect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vpn-sessions.tsv")
	first := vpnSessionEvent{
		UserID:     42,
		Protocol:   "ov",
		InboundTag: "ov-main",
		SessionID:  "ov:first",
		AssignedIP: "10.66.0.2",
		ClientIP:   "198.51.100.10",
		Event:      "start",
	}
	if !vpnAdmitGoSession(path, nil, first, 1) {
		t.Fatal("expected first OpenVPN session to be admitted")
	}
	second := first
	second.SessionID = "ov:second"
	if !vpnAdmitGoSession(path, nil, second, 1) {
		t.Fatal("expected reconnect to replace the previous OpenVPN session")
	}
	records := vpnSessionRecordsLocked(path)
	if len(records) != 1 || records[0][3] != safeName(second.SessionID) {
		t.Fatalf("OpenVPN reconnect was not replaced: %#v", records)
	}
}

func TestVPNSessionLedgerIgnoresLegacySessionWithoutAddresses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vpn-sessions.tsv")
	if err := os.WriteFile(path, []byte("42\tov\tov-main\tov:legacy\t\t\t1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !vpnAdmitGoSession(path, nil, vpnSessionEvent{
		UserID:     42,
		Protocol:   "ov",
		InboundTag: "ov-main",
		SessionID:  "ov:new",
		AssignedIP: "10.66.0.2",
		ClientIP:   "198.51.100.10",
		Event:      "start",
	}, 1) {
		t.Fatal("legacy session without an address must not consume a device slot")
	}
	records := vpnSessionRecordsLocked(path)
	if len(records) != 1 || records[0][3] != safeName("ov:new") {
		t.Fatalf("legacy session was not pruned: %#v", records)
	}
}
