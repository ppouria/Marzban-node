package node

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRemoteAccessUsesNodeWideSessionLedger(t *testing.T) {
	dataDir := t.TempDir()
	manager := newRemoteAccessManager(dataDir, "binary")
	if got, want := manager.sessionsPath(), filepath.Join(dataDir, "vpn-sessions.tsv"); got != want {
		t.Fatalf("sessions path = %q, want %q", got, want)
	}
	if !vpnAdmitGoSession(manager.sessionsPath(), nil, vpnSessionEvent{UserID: 42, Protocol: "wg", InboundTag: "wg-main", SessionID: "wg:one", ClientIP: "198.51.100.10"}, 1) {
		t.Fatal("expected existing protocol session to be admitted")
	}
	if vpnAdmitGoSession(manager.sessionsPath(), nil, vpnSessionEvent{UserID: 42, Protocol: "ikev2", InboundTag: "ike-main", SessionID: "ike:two", ClientIP: "198.51.100.11"}, 1) {
		t.Fatal("expected IKEv2 session to share the node-wide device limit")
	}
	if vpnAdmitGoSession(manager.sessionsPath(), nil, vpnSessionEvent{UserID: 42, Protocol: "anyconnect", InboundTag: "ac-main", SessionID: "ac:three", ClientIP: "198.51.100.12"}, 1) {
		t.Fatal("expected AnyConnect session to share the node-wide device limit")
	}
}

func TestIKEv2ConfigAuthenticationModes(t *testing.T) {
	inbound := remoteAccessRuntimeInbound{Settings: map[string]any{
		"auth_mode": "password+certificate", "server_identity": "vpn.example.com",
		"ipv4_pool_cidr": "10.70.0.0/24", "ike_proposals": "aes256-sha256-modp2048",
		"esp_proposals": "aes256-sha256",
	}}
	config := ikev2IPSecConfig(inbound, "/cert.pem", "/ca.pem")
	for _, expected := range []string{"rightauth=pubkey", "rightauth2=eap-mschapv2", "leftid=vpn.example.com", "rightsourceip=%rebecca-ikev2"} {
		if !strings.Contains(config, expected) {
			t.Fatalf("missing %q in:\n%s", expected, config)
		}
	}
}

func TestParseIKEv2SAs(t *testing.T) {
	raw := `list-sa event {rebecca-ikev2 {uniqueid=7 version=2 state=ESTABLISHED remote-id=alice remote-host=198.51.100.8 child-sas {rebecca-ikev2-1 {bytes-in=1048576 bytes-out=524288 remote-ts=[10.70.0.12/32]}}}}`
	sessions := parseIKEv2SAs(raw)
	if len(sessions) != 1 || sessions[0].Username != "alice" || sessions[0].AssignedIP != "10.70.0.12" || sessions[0].Bytes != 1572864 {
		t.Fatalf("unexpected sessions: %#v", sessions)
	}
}

func TestAnyConnectConfigIsStable(t *testing.T) {
	inbound := remoteAccessRuntimeInbound{Port: 443, Settings: map[string]any{
		"auth_mode": "password", "ipv4_pool_cidr": "10.71.0.0/24", "udp_enabled": true,
		"max_clients": 100, "max_same_clients": 2, "mtu": 1400,
	}}
	first := anyConnectConfig(inbound, "/tmp/ac", "edge")
	for i := 0; i < 20; i++ {
		if next := anyConnectConfig(inbound, "/tmp/ac", "edge"); next != first {
			t.Fatal("ocserv config order is unstable")
		}
	}
	if !strings.Contains(first, `auth = "pam[service=rebecca-ocserv-edge]"`) {
		t.Fatal("missing isolated PAM service")
	}
	if !strings.Contains(first, "device = rac443") || !strings.Contains(first, "connect-script = ") {
		t.Fatal("missing isolated device or active-user gate")
	}
}

func TestParseHumanBytes(t *testing.T) {
	if got := parseHumanBytes("1.5 MB"); got != 1572864 {
		t.Fatalf("got %d", got)
	}
}
