package node

import (
	"testing"
	"time"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

func TestCalculateUserSpeedsAggregatesInboundCounters(t *testing.T) {
	got := calculateUserSpeeds([]xray.UserStat{
		{UID: "42", Up: 500, Down: 1_000},
		{UID: "42", Up: 250, Down: 500},
	}, 5*time.Second)
	if len(got) != 1 || got[0].Upload != 150 || got[0].Download != 300 {
		t.Fatalf("speed=%#v", got)
	}
	if seeded := calculateUserSpeeds([]xray.UserStat{{UID: "42", Up: 500}}, 0); len(seeded) != 0 {
		t.Fatalf("first sample must only seed the clock: %#v", seeded)
	}
}
