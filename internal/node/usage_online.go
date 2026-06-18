package node

import (
	"strings"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

const onlineUserSamplePrefix = "online:"

func appendOnlineUserMarkers(stats []xray.UserStat, onlineUIDs []string) []xray.UserStat {
	if len(onlineUIDs) == 0 {
		return stats
	}

	withTraffic := map[string]struct{}{}
	for _, stat := range stats {
		uid := strings.TrimSpace(stat.UID)
		if uid != "" && stat.Value > 0 {
			withTraffic[uid] = struct{}{}
		}
	}

	for _, uid := range onlineUIDs {
		uid = strings.TrimSpace(uid)
		if uid == "" {
			continue
		}
		if _, ok := withTraffic[uid]; ok {
			continue
		}
		stats = append(stats, xray.UserStat{UID: onlineUserSamplePrefix + uid, Value: 1})
	}
	return stats
}
