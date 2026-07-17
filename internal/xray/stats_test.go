package xray

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	statscommand "github.com/xtls/xray-core/app/stats/command"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type concurrentOnlineIPClient struct {
	active atomic.Int32
	peak   atomic.Int32
	fail   string
}

func (c *concurrentOnlineIPClient) GetStatsOnlineIpList(_ context.Context, request *statscommand.GetStatsRequest, _ ...grpc.CallOption) (*statscommand.GetStatsOnlineIpListResponse, error) {
	if c.fail != "" && strings.Contains(request.GetName(), c.fail) {
		return nil, status.Error(codes.Unavailable, "temporary failure")
	}
	active := c.active.Add(1)
	for peak := c.peak.Load(); active > peak && !c.peak.CompareAndSwap(peak, active); peak = c.peak.Load() {
	}
	time.Sleep(5 * time.Millisecond)
	c.active.Add(-1)
	return &statscommand.GetStatsOnlineIpListResponse{Name: request.GetName(), Ips: map[string]int64{"198.51.100.10": 100}}, nil
}

func TestQueryOnlineUserIPsPreservesPartialResults(t *testing.T) {
	client := &concurrentOnlineIPClient{fail: "2.user"}
	users, err := queryOnlineUserIPs(context.Background(), client, []string{
		"user>>>1.user>>>online", "user>>>2.user>>>online", "user>>>3.user>>>online",
	})
	if err == nil {
		t.Fatal("expected partial query error")
	}
	if len(users) != 2 {
		t.Fatalf("partial users=%d want=2", len(users))
	}
}

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

func TestQueryOnlineUserIPsUsesBoundedWorkers(t *testing.T) {
	names := make([]string, 24)
	for index := range names {
		names[index] = fmt.Sprintf("user>>>%d.user>>>online", index+1)
	}
	client := &concurrentOnlineIPClient{}
	users, err := queryOnlineUserIPs(context.Background(), client, names)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != len(names) {
		t.Fatalf("users=%d want=%d", len(users), len(names))
	}
	if peak := client.peak.Load(); peak < 2 || peak > 8 {
		t.Fatalf("worker peak=%d want 2..8", peak)
	}
}
