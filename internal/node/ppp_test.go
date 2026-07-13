package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestL2TPAndPPTPTProxyMasqueradeICMP(t *testing.T) {
	l2tp := l2tpNFTScript(l2tpRuntimeInbound{
		Tag:        "l2",
		TunnelPort: 1702,
		Settings:   map[string]any{"tproxy_enabled": true, "ipv4_pool_cidr": "10.67.0.0/16"},
	})
	pptp := pptpNFTScript(pptpRuntimeInbound{
		Tag:        "pptp",
		TunnelPort: 41942,
		Settings:   map[string]any{"tproxy_enabled": true, "ipv4_pool_cidr": "10.68.0.0/16"},
	})
	for name, script := range map[string]string{"l2tp": l2tp, "pptp": pptp} {
		for _, want := range []string{
			"meta l4proto { tcp, udp } tproxy ip to 127.0.0.1:",
			"type nat hook postrouting priority srcnat",
			"meta l4proto icmp masquerade",
		} {
			if !strings.Contains(script, want) {
				t.Fatalf("%s script missing %q:\n%s", name, want, script)
			}
		}
	}
}

func TestPPPIPUpRejectsPendingQuota(t *testing.T) {
	script := l2tpIPUpScript("/tmp/users.tsv", "/tmp/usage.tsv", "/tmp/sessions.tsv", "/tmp/callback.env", "/tmp/vpn-sessions.tsv", "l2")
	for _, want := range []string{
		"USAGE=\"/tmp/usage.tsv\"",
		"FILENAME == ARGV[1]",
		"pending[id] += $2",
		"used = $5 + pending[$1]",
		"if ($6 != \"\" && used >= $6) exit 2",
		"\"$USAGE\" \"$USERS\"",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("ip-up script missing %q:\n%s", want, script)
		}
	}
}

func TestPPPIPDownSubtractsAccountedUsage(t *testing.T) {
	script := l2tpIPDownScript("/tmp/users.tsv", "/tmp/usage.tsv", "/tmp/accounting.tsv", "/tmp/sessions.tsv", "/tmp/callback.env", "/tmp/vpn-sessions.tsv", "l2")
	for _, want := range []string{
		"ACCOUNTING=\"/tmp/accounting.tsv\"",
		"previous=$(awk",
		"delta=$((total - previous))",
		"printf 'l2tp:%s\\t%s\\n' \"$uid\" \"$delta\"",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("ip-down script missing %q:\n%s", want, script)
		}
	}
}

func TestPPPCollectUsageReadsLiveInterfaceDeltas(t *testing.T) {
	dir := t.TempDir()
	oldRoot := pppNetStatRoot
	pppNetStatRoot = filepath.Join(dir, "sys")
	defer func() { pppNetStatRoot = oldRoot }()

	base := filepath.Join(dir, "l2tp")
	if err := os.MkdirAll(filepath.Join(pppNetStatRoot, "ppp0", "statistics"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "users.tsv"), []byte("42\talice\tpass\t10.67.0.2\t100\t1000\tactive\t\t0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "sessions.tsv"), []byte("alice\tppp0\t123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(pppNetStatRoot, "ppp0", "statistics", "rx_bytes"), []byte("200\n"), 0o600)
	_ = os.WriteFile(filepath.Join(pppNetStatRoot, "ppp0", "statistics", "tx_bytes"), []byte("300\n"), 0o600)

	stats := newL2TPManager(dir, "binary").CollectUsage()
	if len(stats) != 1 || stats[0].UID != "l2tp:42" || stats[0].Value != 500 {
		t.Fatalf("unexpected first stats: %#v", stats)
	}
	stats = newL2TPManager(dir, "binary").CollectUsage()
	if len(stats) != 0 {
		t.Fatalf("second collect should have no duplicate delta: %#v", stats)
	}
}
