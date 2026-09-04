//go:build linux

package node

import (
	"strings"
	"testing"
)

func TestTelemtConfigUsesInboundModesAndLimits(t *testing.T) {
	config, err := telemtConfig(extraRuntimeInbound{
		Tag: "telegram", Protocol: "mtproto", Listen: "0.0.0.0", Port: 8443,
		Settings: map[string]any{
			"secret": strings.Repeat("ab", 16), "sponsor_tag": strings.Repeat("cd", 16),
			"mode_classic": true, "mode_secure": false, "mode_tls": true,
			"tls_domain": "www.google.com", "user_limit": float64(3), "max_connections": float64(128),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"use_middle_proxy = true", `ad_tag = "` + strings.Repeat("cd", 16) + `"`,
		"classic = true", "secure = false", "tls = true", `rebecca = "classic,tls"`,
		"user_max_tcp_conns_global_each = 128", "rebecca = 3",
	} {
		if !strings.Contains(config, expected) {
			t.Fatalf("generated config does not contain %q:\n%s", expected, config)
		}
	}
	if _, err := telemtConfig(extraRuntimeInbound{Listen: "0.0.0.0", Port: 443, Settings: map[string]any{"secret": strings.Repeat("ab", 16)}}); err == nil {
		t.Fatal("MTProxy config without a connection mode was accepted")
	}
}
