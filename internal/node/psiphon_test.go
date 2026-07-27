package node

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPsiphonConfigUsesAnIsolatedLocalSocksProxy(t *testing.T) {
	content, err := psiphonConfigContents(`{"PropagationChannelId":"channel","SponsorId":"sponsor","DisableLocalHTTPProxy":false}`, "de", 20888)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(content, &config); err != nil {
		t.Fatal(err)
	}
	if config["EgressRegion"] != "DE" || config["LocalSocksProxyPort"] != float64(20888) || config["DisableLocalHTTPProxy"] != true {
		t.Fatalf("unexpected Psiphon config: %#v", config)
	}
}

func TestNormalizedPsiphonLocationsRejectsDuplicates(t *testing.T) {
	_, err := normalizedPsiphonLocations([]string{"de", "DE"})
	if err == nil || !strings.Contains(err.Error(), "unique") {
		t.Fatalf("err=%v", err)
	}
}
