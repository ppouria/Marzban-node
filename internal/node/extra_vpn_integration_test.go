//go:build integration && linux

package node

import (
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestExtraVPNLive(t *testing.T) {
	runtimePath := os.Getenv("REBECCA_EXTRA_VPN_RUNTIME")
	if runtimePath == "" {
		t.Skip("REBECCA_EXTRA_VPN_RUNTIME is not set")
	}
	raw, err := os.ReadFile(runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	var runtimeConfig extraRuntime
	if err := json.Unmarshal(raw, &runtimeConfig); err != nil {
		t.Fatal(err)
	}
	dataDir := os.Getenv("REBECCA_EXTRA_VPN_DATA_DIR")
	if dataDir == "" {
		dataDir = t.TempDir()
	}
	vpn := newExtraVPNManager(dataDir, "binary", func() string { return "dev" })
	ssh := newSSHProxyManager(dataDir)
	if err := ssh.Apply(&runtimeConfig); err != nil {
		t.Fatal(err)
	}
	defer ssh.Apply(&extraRuntime{})
	if err := vpn.Apply(&runtimeConfig); err != nil {
		t.Fatal(err)
	}
	defer vpn.Apply(&extraRuntime{})
	if ready := os.Getenv("REBECCA_EXTRA_VPN_READY"); ready != "" {
		if err := os.WriteFile(ready, []byte("ready\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	seconds, _ := strconv.Atoi(os.Getenv("REBECCA_EXTRA_VPN_SECONDS"))
	if seconds <= 0 {
		seconds = 180
	}
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for time.Now().Before(deadline) {
		_ = vpn.CollectUsage()
		time.Sleep(time.Second)
	}
}
