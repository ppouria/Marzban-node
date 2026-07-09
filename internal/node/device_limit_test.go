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
