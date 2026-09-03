package node

import "github.com/rebeccapanel/rebecca-node/internal/xray"

type userUsageKey struct {
	UID        string
	InboundTag string
}

func addUserUsage(totals map[userUsageKey]int64, uid, inboundTag string, value int64) {
	if uid == "" || value <= 0 {
		return
	}
	key := userUsageKey{UID: uid, InboundTag: inboundTag}
	totals[key] = addUsageCounter(totals[key], value)
}

func userUsageStats(totals map[userUsageKey]int64) []xray.UserStat {
	result := make([]xray.UserStat, 0, len(totals))
	for key, value := range totals {
		if value > 0 {
			result = append(result, xray.UserStat{UID: key.UID, Value: value, InboundTag: key.InboundTag})
		}
	}
	return result
}
