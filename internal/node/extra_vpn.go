package node

import (
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

type extraVPNManager struct {
	baseDir       string
	installMode   string
	updateChannel func() string
	mu            sync.Mutex
	runtime       *extraRuntime
	sstpProcesses map[string]managedProcess
	awgProcesses  map[string]managedProcess
	greLearner    *greLearner
}

type managedProcess struct {
	pid int
}

func newExtraVPNManager(dataDir, installMode string, updateChannel func() string) *extraVPNManager {
	return &extraVPNManager{
		baseDir:       filepath.Join(dataDir, "extra-vpn"),
		installMode:   strings.ToLower(strings.TrimSpace(installMode)),
		updateChannel: updateChannel,
		sstpProcesses: map[string]managedProcess{},
		awgProcesses:  map[string]managedProcess{},
		greLearner:    newGRELearner(),
	}
}

func (m *extraVPNManager) State(protocol string) (configured, running int) {
	if m == nil {
		return 0, 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runtime != nil {
		for _, inbound := range m.runtime.Inbounds {
			if strings.EqualFold(inbound.Protocol, protocol) {
				configured++
			}
		}
	}
	processes := mapForProtocol(m, protocol)
	for tag, process := range processes {
		if processExists(process.pid) {
			running++
		} else {
			delete(processes, tag)
		}
	}
	if protocol == "gre" {
		running = runningGREInbounds(m.runtime)
	}
	return configured, running
}

func mapForProtocol(m *extraVPNManager, protocol string) map[string]managedProcess {
	if strings.EqualFold(protocol, "sstp") {
		return m.sstpProcesses
	}
	if strings.EqualFold(protocol, "amneziawg") {
		return m.awgProcesses
	}
	return nil
}

func (m *extraVPNManager) CollectUsage() []xray.UserStat {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.collectUsageLocked()
}

func (m *extraVPNManager) OnlineUIDs() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]struct{}{}
	result := []string{}
	_ = withVPNSessionLock(vpnSessionsPath(m.baseDir), func() {
		for _, record := range vpnSessionRecordsLocked(vpnSessionsPath(m.baseDir)) {
			if len(record) < 3 {
				continue
			}
			protocol := strings.ToLower(strings.TrimSpace(record[1]))
			if protocol != "sstp" && protocol != "amneziawg" && protocol != "gre" {
				continue
			}
			userID, err := strconv.ParseInt(strings.TrimSpace(record[0]), 10, 64)
			if err != nil || userID <= 0 {
				continue
			}
			uid := protocol + ":" + strconv.FormatInt(userID, 10)
			if _, ok := seen[uid]; !ok {
				seen[uid] = struct{}{}
				result = append(result, uid)
			}
		}
	})
	return result
}
