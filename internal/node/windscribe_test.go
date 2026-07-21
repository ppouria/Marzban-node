package node

import (
	"strings"
	"testing"
)

func TestParseWindscribeLocationsDeduplicatesCountries(t *testing.T) {
	locations := parseWindscribeLocations(`Best Location - Bend
Germany - Frankfurt - Wurstchen
Germany - Berlin - Bear (Pro)
US Central - Atlanta - Piedmont
US East - New York - Empire
Canada East - Toronto - Comfort Zone (Disabled)
Canada West - Montreal - Bagel Poutine`)
	if len(locations) != 3 {
		t.Fatalf("locations=%#v", locations)
	}
	if locations[0].Name != "Canada" || !locations[0].Available {
		t.Fatalf("Canada should be available: %#v", locations[0])
	}
	if locations[1].Name != "Germany" || !locations[1].Available {
		t.Fatalf("Germany should be available: %#v", locations[1])
	}
	if locations[2].Name != "United States" || !locations[2].Available {
		t.Fatalf("United States should be available: %#v", locations[2])
	}
}

func TestSanitizeWindscribeOutputKeepsSuccessfulLocationList(t *testing.T) {
	output := strings.Repeat("Germany - Frankfurt - Wurstchen\n", 30)
	if got := sanitizeWindscribeOutput(output); len(got) <= 600 {
		t.Fatalf("successful CLI output was truncated to %d bytes", len(got))
	}
	if got := truncateWindscribeOutput(output); len(got) != 600 {
		t.Fatalf("diagnostic output length=%d", len(got))
	}
}

func TestDefaultIPv4InterfaceIgnoresTunnelRoutes(t *testing.T) {
	routes := `Iface Destination Gateway Flags RefCnt Use Metric Mask
utun420 00FFFF0A 00000000 0001 0 0 0 00FFFFFF
eth0 00000000 01CAB06D 0003 0 0 10 00000000
eth1 00000000 01CAB06D 0003 0 0 20 00000000`
	got, err := defaultIPv4Interface(strings.NewReader(routes))
	if err != nil {
		t.Fatal(err)
	}
	if got != "eth0" {
		t.Fatalf("default interface=%q", got)
	}
}

func TestWindscribeConfigUsesAuthenticatedSplitTunnelProxy(t *testing.T) {
	config := windscribeProxyConfig{
		SocksPort:     18888,
		ProxyUsername: "RebeccaProxy01",
		ProxyPassword: "RebeccaProxy02",
	}
	content := windscribeConfigContents(config)
	for _, expected := range []string{
		"ShareProxyGatewayMode=SOCKS",
		"ShareProxyGatewayRequireAuth=true",
		"SplitTunnelingMode=Include",
		"SplitTunnelingApps=/opt/windscribe/Windscribe",
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("missing %q in %s", expected, content)
		}
	}
}

func TestValidWindscribeLoginValueRejectsTerminalInput(t *testing.T) {
	for _, value := range []string{"", "short", "user\nlogout", "user\rpassword", "user\x00name"} {
		if validWindscribeLoginValue(value, 8, 256) {
			t.Fatalf("accepted invalid login value %q", value)
		}
	}
	if !validWindscribeLoginValue("correct-password", 8, 256) {
		t.Fatal("rejected valid login value")
	}
}
