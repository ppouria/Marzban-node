//go:build linux

package node

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestExtraVPNProtocolParsersAndSSTPLimiter(t *testing.T) {
	dump := "private\tpublic\t51820\toff\t4\t8\t80\npeer-key\tpsk\t198.51.100.8:32100\t10.73.0.2/32\t1700000000\t12\t34\t25\n"
	peers := parseAWGDump(dump)
	if len(peers) != 1 || peers[0].PublicKey != "peer-key" || peers[0].RX+peers[0].TX != 46 {
		t.Fatalf("unexpected AWG dump: %#v", peers)
	}

	packet := make([]byte, 44)
	packet[0], packet[9] = 0x45, 47
	copy(packet[12:16], []byte{198, 51, 100, 9})
	packet[22], packet[23], packet[24] = 0x08, 0x00, 0x45
	copy(packet[36:40], []byte{10, 74, 0, 2})
	outer, inner, ok := parseGREPair(packet)
	if !ok || outer != "198.51.100.9" || inner != "10.74.0.2" {
		t.Fatalf("GRE pair = %q, %q, %v", outer, inner, ok)
	}

	script := sstpIPUpScript("/tmp/sstp", "/tmp/all-vpn-sessions.tsv", "edge", 23456)
	for _, wanted := range []string{"vpn_admit", "terminate if \"$ifname\" hard", "23456", "/tmp/all-vpn-sessions.tsv"} {
		if !strings.Contains(script, wanted) {
			t.Fatalf("SSTP admission script is missing %q", wanted)
		}
	}

	sessions := filepath.Join(t.TempDir(), "sessions.tsv")
	if !vpnAdmitGoSession(sessions, nil, vpnSessionEvent{UserID: 7, Protocol: "gre", SessionID: "gre-one", AssignedIP: "10.74.0.2", ClientIP: "198.51.100.1"}, 1) ||
		vpnAdmitGoSession(sessions, nil, vpnSessionEvent{UserID: 7, Protocol: "gre", SessionID: "gre-two", AssignedIP: "10.74.0.3", ClientIP: "198.51.100.2"}, 1) {
		t.Fatal("GRE device limit accepted a second client IP")
	}
}
