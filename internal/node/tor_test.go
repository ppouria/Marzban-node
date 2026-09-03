package node

import (
	"strings"
	"testing"
)

func TestTorInstanceConfigKeepsLocationAndPortIndependent(t *testing.T) {
	config := torInstanceConfig(9052, "nl", true, "/var/lib/rebecca-tor/rebecca-tor-9052")
	for _, expected := range []string{
		"DataDirectory /var/lib/rebecca-tor/rebecca-tor-9052",
		"SocksPort 127.0.0.1:9052",
		"ExitNodes {nl}",
		"StrictNodes 1",
		"SocksPolicy reject *",
	} {
		if !strings.Contains(config, expected) {
			t.Fatalf("config does not contain %q:\n%s", expected, config)
		}
	}
}
