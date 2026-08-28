package node

import (
	"time"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

type userTrafficSpeed struct {
	UID      string
	Upload   uint64
	Download uint64
}

func (s *Server) collectXrayUserStats(timeout time.Duration, reset bool) ([]xray.UserStat, []userTrafficSpeed, error) {
	s.userStatsMu.Lock()
	defer s.userStatsMu.Unlock()

	stats, err := xray.QueryUserStats(
		s.settings.XrayAPIHost,
		s.settings.XrayAPIPort,
		timeout,
		reset,
	)
	if err != nil || !reset {
		return stats, nil, err
	}

	now := time.Now()
	elapsed := time.Duration(0)
	if !s.userStatsAt.IsZero() {
		elapsed = now.Sub(s.userStatsAt)
	}
	s.userStatsAt = now
	return stats, calculateUserSpeeds(stats, elapsed), nil
}

func calculateUserSpeeds(stats []xray.UserStat, elapsed time.Duration) []userTrafficSpeed {
	if elapsed <= 0 {
		return nil
	}
	seconds := elapsed.Seconds()
	type counters struct{ upload, download uint64 }
	byUID := map[string]counters{}
	for _, stat := range stats {
		if stat.UID == "" || (stat.Up <= 0 && stat.Down <= 0) {
			continue
		}
		item := byUID[stat.UID]
		if stat.Up > 0 {
			item.upload += uint64(stat.Up)
		}
		if stat.Down > 0 {
			item.download += uint64(stat.Down)
		}
		byUID[stat.UID] = item
	}

	result := make([]userTrafficSpeed, 0, len(byUID))
	for uid, bytes := range byUID {
		result = append(result, userTrafficSpeed{
			UID:      uid,
			Upload:   uint64(float64(bytes.upload) / seconds),
			Download: uint64(float64(bytes.download) / seconds),
		})
	}
	return result
}
