package node

import (
	"strings"
	"testing"
)

func TestVPNDirectNATRulesUseTaggedMasqueradeAndPrivateDrops(t *testing.T) {
	rules := vpnDirectNATRules("openvpn-ov", "rbov12345678", "10.66.0.0/16", "eth0")
	joined := make([]string, 0, len(rules))
	for _, rule := range rules {
		joined = append(joined, rule.table+" "+rule.chain+" "+strings.Join(rule.spec, " "))
	}
	text := strings.Join(joined, "\n")
	for _, want := range []string{
		"-s 10.66.0.0/16 -o eth0 -j MASQUERADE",
		"-i rbov12345678 -d 10.0.0.0/8 -j REJECT",
		"--comment rebecca-vpn:openvpn-ov",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("direct NAT rules missing %q:\n%s", want, text)
		}
	}
}
