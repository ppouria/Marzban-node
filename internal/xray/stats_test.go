package xray

import "testing"

func TestParseOutboundStatName(t *testing.T) {
	tag, link, ok := parseOutboundStatName("outbound>>>proxy>>>traffic>>>uplink")
	if !ok || tag != "proxy" || link != "uplink" {
		t.Fatalf("unexpected parse result: tag=%q link=%q ok=%v", tag, link, ok)
	}

	if _, _, ok := parseOutboundStatName("user>>>1.test>>>traffic>>>uplink"); ok {
		t.Fatal("non-outbound stats should be ignored")
	}
}

func TestParseUserStatName(t *testing.T) {
	uid, ok := parseUserStatName("user>>>42.alice>>>traffic>>>downlink")
	if !ok || uid != "42" {
		t.Fatalf("unexpected parse result: uid=%q ok=%v", uid, ok)
	}

	if _, ok := parseUserStatName("outbound>>>proxy>>>traffic>>>uplink"); ok {
		t.Fatal("non-user stats should be ignored")
	}
}

func TestParseOnlineUserName(t *testing.T) {
	tests := []struct {
		name  string
		email string
		uid   string
		ok    bool
	}{
		{name: "user>>>abc123.example>>>online", email: "abc123.example", uid: "abc123", ok: true},
		{name: "abc123.example", email: "abc123.example", uid: "abc123", ok: true},
		{name: "user>>>>>>online", ok: false},
		{name: "", ok: false},
	}

	for _, tt := range tests {
		email, uid, ok := parseOnlineUserName(tt.name)
		if ok != tt.ok || email != tt.email || uid != tt.uid {
			t.Fatalf("parseOnlineUserName(%q) = (%q, %q, %v), want (%q, %q, %v)", tt.name, email, uid, ok, tt.email, tt.uid, tt.ok)
		}
	}
}

func TestParseUserEmailUID(t *testing.T) {
	uid, ok := parseUserEmailUID("42.alice")
	if !ok || uid != "42" {
		t.Fatalf("unexpected parse result: uid=%q ok=%v", uid, ok)
	}

	uid, ok = parseUserEmailUID("42")
	if !ok || uid != "42" {
		t.Fatalf("unexpected parse result without suffix: uid=%q ok=%v", uid, ok)
	}

	if _, ok := parseUserEmailUID(""); ok {
		t.Fatal("empty email should be ignored")
	}
}
