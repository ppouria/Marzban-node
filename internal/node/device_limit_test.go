package node

import (
	"strings"
	"testing"
)

func intPtr(v int64) *int64 { return &v }

func TestOVServerConfigAllowsDuplicateCN(t *testing.T) {
	config := serverConfig(ovRuntimeInbound{
		Tag:       "ov",
		Port:      1194,
		Transport: "udp",
		Settings:  map[string]any{"transport": "udp"},
	}, "/tmp/ov", "/tmp/ov/ccd")

	if !strings.Contains(config, "duplicate-cn\n") {
		t.Fatalf("server config must allow duplicate-cn so a device limit above one can connect:\n%s", config)
	}
}

func TestOVUsersTSVCarriesDeviceLimit(t *testing.T) {
	tsv := usersTSV([]ovRuntimeUser{{
		UserID:      7,
		VPNUsername: "alice",
		Password:    "pw",
		Status:      "active",
		DeviceLimit: intPtr(2),
	}})
	fields := strings.Split(strings.TrimRight(tsv, "\n"), "\t")
	if len(fields) != 9 {
		t.Fatalf("expected 9 TSV columns, got %d: %q", len(fields), fields)
	}
	if fields[8] != "2" {
		t.Fatalf("device limit column = %q, want 2", fields[8])
	}
}

func TestOVUsersTSVOmitsUnsetDeviceLimit(t *testing.T) {
	tsv := usersTSV([]ovRuntimeUser{{UserID: 7, VPNUsername: "alice", Password: "pw", Status: "active"}})
	fields := strings.Split(strings.TrimRight(tsv, "\n"), "\t")
	if fields[8] != "" {
		t.Fatalf("unset device limit column = %q, want empty", fields[8])
	}
}

func TestOVAuthScriptCountsActiveConnections(t *testing.T) {
	script := authScript("/tmp/ov/users.tsv", "/tmp/ov/status.log")
	// It must read the status log and reject once the active count reaches the
	// limit; a distinctive exit code (4) marks the device-limit rejection.
	for _, want := range []string{"STATUS=", "CLIENT_LIST", `"$active" -ge "$limit"`, "exit 4"} {
		if !strings.Contains(script, want) {
			t.Fatalf("auth script missing %q:\n%s", want, script)
		}
	}
}

func TestL2TPIPUpScriptEnforcesDeviceLimit(t *testing.T) {
	script := l2tpIPUpScript("/tmp/l2tp/sessions.tsv", "/tmp/l2tp/users.tsv")
	// Sessions must be keyed by interface (so one user can hold several) and the
	// new session rejected by tearing down its pppd once the limit is reached.
	for _, want := range []string{"USERS=", `$2 != i`, `"$active" -ge "$limit"`, "kill -TERM"} {
		if !strings.Contains(script, want) {
			t.Fatalf("ip-up script missing %q:\n%s", want, script)
		}
	}
}

func TestL2TPUsersTSVCarriesDeviceLimit(t *testing.T) {
	tsv := l2tpUsersTSV([]l2tpRuntimeUser{{
		UserID:      3,
		VPNUsername: "bob",
		Password:    "pw",
		Status:      "active",
		DeviceLimit: intPtr(5),
	}})
	fields := strings.Split(strings.TrimRight(tsv, "\n"), "\t")
	if len(fields) != 9 || fields[8] != "5" {
		t.Fatalf("device limit column = %q (fields=%d), want 5/9", fields[8], len(fields))
	}
}

func TestL2TPChapSecretsUsesPoolForMultiDevice(t *testing.T) {
	users := []l2tpRuntimeUser{
		{VPNUsername: "single", Password: "pw", IPv4Address: "10.67.0.5", DeviceLimit: intPtr(1)},
		{VPNUsername: "multi", Password: "pw", IPv4Address: "10.67.0.6", DeviceLimit: intPtr(3)},
	}
	out := l2tpChapSecrets(users)
	if !strings.Contains(out, `"single" rebecca-l2tp "pw" 10.67.0.5`) {
		t.Fatalf("single-device user should keep its pinned IP:\n%s", out)
	}
	if !strings.Contains(out, `"multi" rebecca-l2tp "pw" *`) {
		t.Fatalf("multi-device user should draw from the pool (*):\n%s", out)
	}
}

func TestWGDeviceLimitCapsActivePeersPerUser(t *testing.T) {
	// User 1 is capped at 2 devices but sends 3 active peers; user 2 is unlimited.
	peers := []wgRuntimePeer{
		{UserID: 1, PublicKey: "kC", Address: "10.70.0.4", Status: "active", DeviceLimit: intPtr(2)},
		{UserID: 1, PublicKey: "kA", Address: "10.70.0.2", Status: "active", DeviceLimit: intPtr(2)},
		{UserID: 1, PublicKey: "kB", Address: "10.70.0.3", Status: "active", DeviceLimit: intPtr(2)},
		{UserID: 2, PublicKey: "zz", Address: "10.70.0.9", Status: "active"},
	}
	allowed := wgAllowedByDeviceLimit(peers)
	if len(allowed) != 3 {
		t.Fatalf("expected 3 allowed peers (2 capped + 1 unlimited), got %d: %v", len(allowed), allowed)
	}
	// Selection is by public-key order, so kA and kB win, kC is dropped.
	if _, ok := allowed["kC"]; ok {
		t.Fatalf("kC should be dropped as the surplus peer over the limit: %v", allowed)
	}
	for _, want := range []string{"kA", "kB", "zz"} {
		if _, ok := allowed[want]; !ok {
			t.Fatalf("expected %q to stay on the interface: %v", want, allowed)
		}
	}
}

func TestWGDeviceLimitUnsetIsUnlimited(t *testing.T) {
	peers := []wgRuntimePeer{
		{UserID: 5, PublicKey: "a", Address: "10.70.0.2", Status: "active"},
		{UserID: 5, PublicKey: "b", Address: "10.70.0.3", Status: "active"},
	}
	if allowed := wgAllowedByDeviceLimit(peers); len(allowed) != 2 {
		t.Fatalf("unset limit must keep all peers, got %d: %v", len(allowed), allowed)
	}
}

func TestWGDeviceLimitIgnoresInactivePeers(t *testing.T) {
	peers := []wgRuntimePeer{
		{UserID: 9, PublicKey: "live", Address: "10.70.0.2", Status: "active", DeviceLimit: intPtr(1)},
		{UserID: 9, PublicKey: "dead", Address: "10.70.0.3", Status: "disabled", DeviceLimit: intPtr(1)},
	}
	allowed := wgAllowedByDeviceLimit(peers)
	if _, ok := allowed["live"]; !ok || len(allowed) != 1 {
		t.Fatalf("only the active peer should count toward the limit: %v", allowed)
	}
}
