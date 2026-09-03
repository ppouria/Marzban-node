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

func TestParsePsiphonAvailableRegions(t *testing.T) {
	regions, reported := parsePsiphonAvailableRegions([]byte(`{"noticeType":"AvailableEgressRegions","data":{"regions":["US","de","DE","invalid"]}}`))
	if !reported {
		t.Fatal("notice was not reported")
	}
	if strings.Join(regions, ",") != "de,us" {
		t.Fatalf("regions=%v", regions)
	}
}
