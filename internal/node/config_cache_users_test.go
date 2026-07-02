package node

import (
	"encoding/json"
	"testing"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

func TestPatchConfigCacheUserJSONAddsAndReplacesUser(t *testing.T) {
	raw := `{"inbounds":[{"tag":"vless-ws","protocol":"vless","settings":{"clients":[{"email":"1.old","id":"old-id"}]}}]}`

	updated, changed, err := patchConfigCacheUserJSON(raw, "vless-ws", xray.InboundUser{
		Protocol: "vless",
		Email:    "2.alice",
		ID:       "11111111-1111-4111-8111-111111111111",
		Flow:     "xtls-rprx-vision",
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected cache to change")
	}
	clients := cacheTestClients(t, updated)
	if len(clients) != 2 {
		t.Fatalf("expected two clients, got %#v", clients)
	}
	if clients[1]["email"] != "2.alice" || clients[1]["id"] != "11111111-1111-4111-8111-111111111111" || clients[1]["flow"] != "xtls-rprx-vision" {
		t.Fatalf("unexpected added client: %#v", clients[1])
	}

	replaced, changed, err := patchConfigCacheUserJSON(updated, "vless-ws", xray.InboundUser{
		Protocol: "vless",
		Email:    "2.alice",
		ID:       "22222222-2222-4222-8222-222222222222",
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected replacement to change cache")
	}
	clients = cacheTestClients(t, replaced)
	if len(clients) != 2 {
		t.Fatalf("expected replacement without duplicate, got %#v", clients)
	}
	if clients[1]["id"] != "22222222-2222-4222-8222-222222222222" {
		t.Fatalf("expected replacement id, got %#v", clients[1])
	}
}

func TestPatchConfigCacheUserJSONRemovesUser(t *testing.T) {
	raw := `{"inbounds":[{"tag":"vless-ws","protocol":"vless","settings":{"clients":[{"email":"1.old","id":"old-id"},{"email":"2.alice","id":"alice-id"}]}}]}`

	updated, changed, err := patchConfigCacheUserJSON(raw, "vless-ws", xray.InboundUser{}, "2.alice")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected cache to change")
	}
	clients := cacheTestClients(t, updated)
	if len(clients) != 1 {
		t.Fatalf("expected one remaining client, got %#v", clients)
	}
	if clients[0]["email"] != "1.old" {
		t.Fatalf("unexpected remaining client: %#v", clients[0])
	}
}

func cacheTestClients(t *testing.T, raw string) []map[string]any {
	t.Helper()
	var config struct {
		Inbounds []struct {
			Settings struct {
				Clients []map[string]any `json:"clients"`
			} `json:"settings"`
		} `json:"inbounds"`
	}
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		t.Fatal(err)
	}
	if len(config.Inbounds) != 1 {
		t.Fatalf("expected one inbound, got %#v", config.Inbounds)
	}
	return config.Inbounds[0].Settings.Clients
}
