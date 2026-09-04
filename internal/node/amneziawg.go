//go:build linux

package node

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

const awgSessionTimeout = 3 * time.Minute

type awgPeerSnapshot struct {
	PublicKey string
	Endpoint  string
	Address   string
	Handshake int64
	RX        int64
	TX        int64
}

type awgUsageState struct {
	Counters map[string]int64          `json:"counters"`
	Sessions map[string]wgSessionState `json:"sessions"`
}

func (m *extraVPNManager) applyAWGLocked(inbounds []extraRuntimeInbound, callback *vpnSessionCallback) error {
	for tag, process := range m.awgProcesses {
		iface := awgIfaceName(tag)
		stopManagedProcess(process)
		_ = exec.Command("ip", "link", "del", "dev", iface).Run()
		_ = wgRemoveTProxy(context.Background(), iface)
		_ = wgRemoveNAT(context.Background(), iface)
		delete(m.awgProcesses, tag)
	}
	if len(inbounds) == 0 {
		return nil
	}
	binDir := filepath.Join(m.baseDir, "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		return err
	}
	awgGo, awg := filepath.Join(binDir, "amneziawg-go"), filepath.Join(binDir, "awg")
	if err := m.installAsset("amneziawg-go", awgGo, 1<<20, 100<<20); err != nil {
		return err
	}
	if err := m.installAsset("amneziawg-tools", awg, 100<<10, 20<<20); err != nil {
		return err
	}
	if err := ensureWGPrerequisites(); err != nil {
		return err
	}
	for _, inbound := range inbounds {
		if err := m.startAWGLocked(awgGo, awg, inbound); err != nil {
			return fmt.Errorf("AmneziaWG inbound %s: %w", inbound.Tag, err)
		}
	}
	return nil
}

func (m *extraVPNManager) startAWGLocked(awgGo, awg string, inbound extraRuntimeInbound) error {
	if inbound.Port < 1 || inbound.Port > 65535 {
		return fmt.Errorf("listen port must be between 1 and 65535")
	}
	iface := awgIfaceName(inbound.Tag)
	dir := filepath.Join(m.baseDir, "amneziawg", safeName(inbound.Tag))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	command := exec.Command(awgGo, "--foreground", iface)
	command.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
	logFile, err := os.OpenFile(filepath.Join(dir, "amneziawg.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	cleanup := func() { stopManagedProcess(managedProcess{pid: command.Process.Pid}); _ = logFile.Close() }
	deadline := time.Now().Add(5 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		if wgLinkExists(context.Background(), iface) && exec.Command(awg, "show", iface).Run() == nil {
			ready = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		cleanup()
		return fmt.Errorf("userspace interface did not become ready")
	}
	config, err := awgConfig(inbound)
	if err != nil {
		cleanup()
		return err
	}
	configPath := filepath.Join(dir, "awg.conf")
	if err := writeFileAtomic(configPath, []byte(config), 0o600); err != nil {
		cleanup()
		return err
	}
	if output, err := exec.Command(awg, "setconf", iface, configPath).CombinedOutput(); err != nil {
		cleanup()
		return fmt.Errorf("apply configuration: %v: %s", err, strings.TrimSpace(string(output)))
	}
	pool := firstString(inbound.Settings["ipv4_pool_cidr"], "10.73.0.0/16")
	serverAddress := firstString(inbound.Settings["server_address"])
	if serverAddress == "" {
		serverAddress, err = wgServerAddress(pool)
		if err != nil {
			cleanup()
			return err
		}
	}
	mtu := intSetting(inbound.Settings, "mtu", wgDefaultMTU)
	if output, err := exec.Command("ip", "addr", "replace", serverAddress, "dev", iface).CombinedOutput(); err != nil {
		cleanup()
		return fmt.Errorf("set server address: %v: %s", err, strings.TrimSpace(string(output)))
	}
	if output, err := exec.Command("ip", "link", "set", "dev", iface, "mtu", strconv.Itoa(mtu), "up").CombinedOutput(); err != nil {
		cleanup()
		return fmt.Errorf("bring interface up: %v: %s", err, strings.TrimSpace(string(output)))
	}
	converted := wgRuntimeInbound{Tag: inbound.Tag, TunnelTag: inbound.TunnelTag, ListenPort: inbound.Port, TunnelPort: inbound.TunnelPort, Settings: inbound.Settings, Peers: inbound.Peers}
	if inbound.TunnelPort > 0 && boolValue(inbound.Settings["tproxy_enabled"], true) {
		if err := wgApplyTProxy(context.Background(), m.baseDir, converted, iface); err != nil {
			cleanup()
			return err
		}
		_ = wgRemoveNAT(context.Background(), iface)
	} else {
		if err := wgApplyNAT(context.Background(), iface, wgNetwork(pool)); err != nil {
			cleanup()
			return err
		}
		_ = wgRemoveTProxy(context.Background(), iface)
	}
	m.awgProcesses[inbound.Tag] = managedProcess{pid: command.Process.Pid}
	go func() { _ = command.Wait(); _ = logFile.Close() }()
	return nil
}

func awgConfig(inbound extraRuntimeInbound) (string, error) {
	privateKey := stringSetting(inbound.Settings, "private_key")
	if privateKey == "" {
		return "", fmt.Errorf("private_key is required")
	}
	var config strings.Builder
	fmt.Fprintf(&config, "[Interface]\nPrivateKey = %s\nListenPort = %d\n", privateKey, inbound.Port)
	for _, item := range []struct{ name, key string }{{"Jc", "jc"}, {"Jmin", "jmin"}, {"Jmax", "jmax"}, {"S1", "s1"}, {"S2", "s2"}, {"H1", "h1"}, {"H2", "h2"}, {"H3", "h3"}, {"H4", "h4"}} {
		fmt.Fprintf(&config, "%s = %d\n", item.name, intSetting(inbound.Settings, item.key, 0))
	}
	for _, peer := range inbound.Peers {
		if !wgPeerActive(peer) {
			continue
		}
		address := wgPeerAddressHost(peer.Address)
		if net.ParseIP(address) == nil {
			return "", fmt.Errorf("peer %d has an invalid address", peer.UserID)
		}
		fmt.Fprintf(&config, "\n[Peer]\nPublicKey = %s\nAllowedIPs = %s/32\n", peer.PublicKey, address)
		if strings.TrimSpace(peer.PresharedKey) != "" {
			fmt.Fprintf(&config, "PresharedKey = %s\n", peer.PresharedKey)
		}
	}
	return config.String(), nil
}

func (m *extraVPNManager) collectAWGUsageLocked() []xray.UserStat {
	if m.runtime == nil {
		return nil
	}
	totals := map[userUsageKey]int64{}
	awg := filepath.Join(m.baseDir, "bin", "awg")
	for _, inbound := range filterExtraVPNInbounds(m.runtime.Inbounds, "amneziawg") {
		iface := awgIfaceName(inbound.Tag)
		output, err := exec.Command(awg, "show", iface, "dump").Output()
		if err != nil {
			continue
		}
		peers := parseAWGDump(string(output))
		byKey := map[string]wgRuntimePeer{}
		for _, peer := range inbound.Peers {
			byKey[strings.TrimSpace(peer.PublicKey)] = peer
		}
		statePath := filepath.Join(m.baseDir, "amneziawg", safeName(inbound.Tag), "usage.json")
		state := loadAWGUsageState(statePath)
		nextCounters, nextSessions := map[string]int64{}, map[string]wgSessionState{}
		for _, snapshot := range peers {
			peer, ok := byKey[snapshot.PublicKey]
			if !ok {
				continue
			}
			current := snapshot.RX + snapshot.TX
			nextCounters[snapshot.PublicKey] = current
			if previous, ok := state.Counters[snapshot.PublicKey]; ok && current >= previous && current > previous {
				addUserUsage(totals, "amneziawg:"+strconv.FormatInt(peer.UserID, 10), inbound.Tag, current-previous)
			}
			if peer.DataLimit != nil && *peer.DataLimit > 0 && peer.UsedTraffic+current >= *peer.DataLimit {
				_ = exec.Command(awg, "set", iface, "peer", snapshot.PublicKey, "remove").Run()
				continue
			}
			clientIP := endpointHost(snapshot.Endpoint)
			if snapshot.Handshake <= 0 || time.Since(time.Unix(snapshot.Handshake, 0)) > awgSessionTimeout {
				continue
			}
			sessionID := "awg:" + iface + ":" + snapshot.PublicKey
			previous, existed := state.Sessions[snapshot.PublicKey]
			if existed && previous.ClientIP != clientIP {
				vpnReleaseGoSession(vpnSessionsPath(m.baseDir), m.runtime.SessionCallback, vpnSessionEvent{UserID: peer.UserID, Protocol: "amneziawg", InboundTag: inbound.Tag, SessionID: sessionID, AssignedIP: wgPeerAddressHost(peer.Address), ClientIP: previous.ClientIP, Event: "stop"})
				existed = false
			}
			if !existed {
				event := vpnSessionEvent{UserID: peer.UserID, Protocol: "amneziawg", InboundTag: inbound.Tag, SessionID: sessionID, AssignedIP: wgPeerAddressHost(peer.Address), ClientIP: clientIP, Event: "start"}
				if !vpnAdmitGoSession(vpnSessionsPath(m.baseDir), m.runtime.SessionCallback, event, peer.DeviceLimit) {
					_ = exec.Command(awg, "set", iface, "peer", snapshot.PublicKey, "remove").Run()
					continue
				}
			}
			nextSessions[snapshot.PublicKey] = wgSessionState{UserID: peer.UserID, SessionID: sessionID, Address: peer.Address, ClientIP: clientIP, LastHandshakeUnix: snapshot.Handshake}
		}
		for key, previous := range state.Sessions {
			if _, ok := nextSessions[key]; !ok {
				vpnReleaseGoSession(vpnSessionsPath(m.baseDir), m.runtime.SessionCallback, vpnSessionEvent{UserID: previous.UserID, Protocol: "amneziawg", InboundTag: inbound.Tag, SessionID: previous.SessionID, AssignedIP: wgPeerAddressHost(previous.Address), ClientIP: previous.ClientIP, Event: "stop"})
			}
		}
		saveAWGUsageState(statePath, awgUsageState{Counters: nextCounters, Sessions: nextSessions})
	}
	return userUsageStats(totals)
}

func parseAWGDump(raw string) []awgPeerSnapshot {
	result := []awgPeerSnapshot{}
	scanner := bufio.NewScanner(strings.NewReader(raw))
	first := true
	for scanner.Scan() {
		fields := strings.Split(strings.TrimSpace(scanner.Text()), "\t")
		if first {
			first = false
			continue
		}
		if len(fields) < 8 {
			continue
		}
		handshake, _ := strconv.ParseInt(fields[4], 10, 64)
		rx, _ := strconv.ParseInt(fields[5], 10, 64)
		tx, _ := strconv.ParseInt(fields[6], 10, 64)
		result = append(result, awgPeerSnapshot{PublicKey: fields[0], Endpoint: fields[2], Address: fields[3], Handshake: handshake, RX: rx, TX: tx})
	}
	return result
}

func endpointHost(endpoint string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(endpoint))
	if err == nil {
		return host
	}
	return strings.TrimSpace(endpoint)
}

func loadAWGUsageState(path string) awgUsageState {
	state := awgUsageState{Counters: map[string]int64{}, Sessions: map[string]wgSessionState{}}
	raw, err := os.ReadFile(path)
	if err == nil {
		_ = json.Unmarshal(raw, &state)
	}
	if state.Counters == nil {
		state.Counters = map[string]int64{}
	}
	if state.Sessions == nil {
		state.Sessions = map[string]wgSessionState{}
	}
	return state
}

func saveAWGUsageState(path string, state awgUsageState) {
	raw, err := json.Marshal(state)
	if err == nil {
		_ = writeFileAtomic(path, raw, 0o600)
	}
}

func awgIfaceName(tag string) string { return wgIfaceName("awg:" + tag) }
