package node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

func TestVirtualTunnelCollectorsAttachInboundTags(t *testing.T) {
	t.Run("l2tp", func(t *testing.T) {
		dataDir := t.TempDir()
		manager := newL2TPManager(dataDir, "binary")
		writeUsageFixture(t, manager.baseDir, `{"inbounds":[{"tag":"l2-main"}]}`, "l2tp:42\t10\n")
		assertTaggedUsage(t, manager.CollectUsage(), "l2tp:42", "l2-main", 10)
	})

	t.Run("pptp", func(t *testing.T) {
		dataDir := t.TempDir()
		manager := newPPTPManager(dataDir, "binary")
		writeUsageFixture(t, manager.baseDir, `{"inbounds":[{"tag":"pptp-main"}]}`, "pptp:42\t20\n")
		assertTaggedUsage(t, manager.CollectUsage(), "pptp:42", "pptp-main", 20)
	})

	t.Run("ikev2", func(t *testing.T) {
		dataDir := t.TempDir()
		manager := newRemoteAccessManager(dataDir, "binary")
		dir := filepath.Join(manager.baseDir, "ikev2")
		writeUsageFixture(t, dir, `{"inbounds":[{"tag":"ike-main"}]}`, "42\t30\n")
		assertTaggedUsage(t, manager.CollectUsage("ikev2"), "ikev2:42", "ike-main", 30)
	})

	t.Run("anyconnect", func(t *testing.T) {
		dataDir := t.TempDir()
		manager := newRemoteAccessManager(dataDir, "binary")
		base := filepath.Join(manager.baseDir, "anyconnect")
		if err := os.MkdirAll(filepath.Join(base, "ac-edge"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(base, "runtime.json"), []byte(`{"inbounds":[{"tag":"ac-edge"}]}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(base, "ac-edge", "usage.tsv"), []byte("42\t40\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		assertTaggedUsage(t, manager.CollectUsage("anyconnect"), "anyconnect:42", "ac-edge", 40)
	})
}

func writeUsageFixture(t *testing.T, dir, runtimeJSON, usage string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "runtime.json"), []byte(runtimeJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "usage.tsv"), []byte(usage), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertTaggedUsage(t *testing.T, stats []xray.UserStat, uid, inboundTag string, value int64) {
	t.Helper()
	if len(stats) != 1 || stats[0].UID != uid || stats[0].InboundTag != inboundTag || stats[0].Value != value {
		t.Fatalf("stats=%#v", stats)
	}
}
