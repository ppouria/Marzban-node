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

func TestConfigUserDiffAddsUpdatesAndRemovesUsers(t *testing.T) {
	cached := `{"inbounds":[{"tag":"vless-ws","protocol":"vless","settings":{"clients":[{"email":"1.old","id":"old-id"},{"email":"2.alice","id":"alice-id"}]}}]}`
	incoming := `{"inbounds":[{"tag":"vless-ws","protocol":"vless","settings":{"clients":[{"email":"2.alice","id":"alice-new-id","flow":"xtls-rprx-vision"},{"email":"3.bob","id":"bob-id"}]}}]}`

	diff, err := configUserDiff(cached, incoming)
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.remove) != 2 {
		t.Fatalf("expected one deleted and one replaced user removal, got %#v", diff.remove)
	}
	if diff.remove[0].inboundTag != "vless-ws" || diff.remove[0].email != "1.old" {
		t.Fatalf("unexpected deleted user removal: %#v", diff.remove[0])
	}
	if diff.remove[1].inboundTag != "vless-ws" || diff.remove[1].email != "2.alice" {
		t.Fatalf("unexpected replaced user removal: %#v", diff.remove[1])
	}
	if len(diff.add) != 2 {
		t.Fatalf("expected replaced and new user additions, got %#v", diff.add)
	}
	adds := map[string]configUserAdd{}
	for _, item := range diff.add {
		adds[item.user.Email] = item
	}
	if item := adds["2.alice"]; item.inboundTag != "vless-ws" || item.user.ID != "alice-new-id" || item.user.Flow != "xtls-rprx-vision" {
		t.Fatalf("unexpected replaced user addition: %#v", item)
	}
	if item := adds["3.bob"]; item.inboundTag != "vless-ws" || item.user.ID != "bob-id" {
		t.Fatalf("unexpected new user addition: %#v", item)
	}
}

func TestConfigUserDiffKeepsVLESSReverseTag(t *testing.T) {
	cached := `{"inbounds":[{"tag":"vless-ws","protocol":"vless","settings":{"clients":[]}}]}`
	incoming := `{"inbounds":[{"tag":"vless-ws","protocol":"vless","settings":{"clients":[{"email":"reverse.tunnel","id":"11111111-1111-4111-8111-111111111111","reverse":{"tag":"reverse-out"}}]}}]}`

	diff, err := configUserDiff(cached, incoming)
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.add) != 1 || diff.add[0].user.ReverseTag != "reverse-out" {
		t.Fatalf("reverse tag was not added to the runtime diff: %#v", diff.add)
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
