package node

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

const (
	wgDefaultMTU           = 1420
	wgDefaultPool          = "10.70.0.0/16"
	wgListenPortLo         = 51820
	wgListenPortHi         = 51920
	wgSessionTimeout       = 3 * time.Minute
	wgAdmissionRetryDelay  = 30 * time.Second
	wgIfacePrefix          = "rbwg"
	wgCommandTimeout       = 15 * time.Second
	wgUsagePrefix          = "wg"
	wgReservedForwardChain = "rebecca_wg"
)

type wgRuntime struct {
	GeneratedAt     string              `json:"generated_at"`
	Target          string              `json:"target,omitempty"`
	SessionCallback *vpnSessionCallback `json:"session_callback,omitempty"`
	Inbounds        []wgRuntimeInbound  `json:"inbounds"`
}

type wgRuntimeInbound struct {
	Tag        string          `json:"tag"`
	TunnelTag  string          `json:"tunnel_tag,omitempty"`
	ListenPort int             `json:"listen_port"`
	TunnelPort int             `json:"tunnel_port,omitempty"`
	Settings   map[string]any  `json:"settings"`
	Peers      []wgRuntimePeer `json:"peers"`
}

type wgRuntimePeer struct {
	UserID       int64  `json:"user_id"`
	Username     string `json:"username"`
	PublicKey    string `json:"public_key"`
	PresharedKey string `json:"preshared_key,omitempty"`
	Address      string `json:"address"`
	Status       string `json:"status"`
	UsedTraffic  int64  `json:"used_traffic"`
	DataLimit    *int64 `json:"data_limit,omitempty"`
	Expire       *int64 `json:"expire,omitempty"`
	DeviceLimit  int64  `json:"device_limit,omitempty"`
}

type wgManager struct {
	baseDir     string
	installMode string
	mu          sync.Mutex
	deniedUntil map[string]time.Time
}

func newWGManager(dataDir string, installMode string) *wgManager {
	return &wgManager{
		baseDir:     filepath.Join(dataDir, "wireguard"),
		installMode: strings.ToLower(strings.TrimSpace(installMode)),
		deniedUntil: map[string]time.Time{},
	}
}

// Apply reconciles the live WireGuard state to the desired runtime. Each inbound
// owns its own kernel interface, UDP listen port and NAT rules, so multiple
// inbounds coexist on distinct ports. Inbounds that disappear from the runtime
// are torn down (interface + NAT). Idempotent: safe to call on every push.
func (m *wgManager) Apply(runtimeConfig *wgRuntime) error {
	if m == nil || runtimeConfig == nil {
		return nil
	}
	if len(runtimeConfig.Inbounds) > 0 && m.installMode != "binary" {
		return fmt.Errorf("WireGuard is supported only on binary Rebecca-node installs")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(runtimeConfig.Inbounds) > 0 {
		if err := ensureWGPrerequisites(); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(m.baseDir, 0o700); err != nil {
		return err
	}
	for i := range runtimeConfig.Inbounds {
		runtimeConfig.Inbounds[i] = normalizeWGRuntimeInbound(runtimeConfig.Inbounds[i])
	}
	if err := wgValidateInbounds(runtimeConfig.Inbounds); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(runtimeConfig, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(m.baseDir, "runtime.json"), raw, 0o600); err != nil {
		return err
	}

	desired := make(map[string]struct{}, len(runtimeConfig.Inbounds))
	for _, inbound := range runtimeConfig.Inbounds {
		desired[wgIfaceName(inbound.Tag)] = struct{}{}
	}
	if err := m.pruneRemovedInbounds(desired); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return nil
	}

	client, err := wgctrl.New()
	if err != nil {
		return fmt.Errorf("open wgctrl (is the wireguard kernel module loaded?): %w", err)
	}
	defer client.Close()

	for _, inbound := range runtimeConfig.Inbounds {
		if err := m.applyInbound(client, inbound); err != nil {
			return err
		}
	}
	return nil
}

// CollectUsage returns per-user rx+tx deltas since the previous collection. The
// kernel reports cumulative counters, so we diff against the baselines persisted
// per interface. A counter that moved backwards (interface recreated, peer
// re-keyed) is treated as a fresh baseline so we never report a negative delta.
func (m *wgManager) CollectUsage() []xray.UserStat {
	if m == nil || runtime.GOOS == "windows" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	client, err := wgctrl.New()
	if err != nil {
		return nil
	}
	defer client.Close()

	runtimeConfig := m.currentRuntime()
	if runtimeConfig == nil || len(runtimeConfig.Inbounds) == 0 {
		return nil
	}
	callback := runtimeConfig.SessionCallback

	totals := map[string]int64{}
	for _, inbound := range runtimeConfig.Inbounds {
		iface := wgIfaceName(inbound.Tag)
		device, err := client.Device(iface)
		if err != nil {
			continue
		}
		userByKey := map[string]int64{}
		peerByKey := map[string]wgRuntimePeer{}
		for _, peer := range inbound.Peers {
			key := strings.TrimSpace(peer.PublicKey)
			if key != "" {
				userByKey[key] = peer.UserID
				peerByKey[key] = peer
			}
		}
		m.syncPeerSessions(client, iface, inbound, device, peerByKey, callback)
		baselines := m.loadBaselines(iface)
		usageBases := m.loadUsageBases(iface)
		next := map[string]int64{}
		nextUsageBases := map[string]int64{}
		for _, peer := range device.Peers {
			key := peer.PublicKey.String()
			current := peer.ReceiveBytes + peer.TransmitBytes
			next[key] = current
			userID, ok := userByKey[key]
			if !ok {
				continue
			}
			runtimePeer := peerByKey[key]
			usageBase, seenUsageBase := usageBases[key]
			if !seenUsageBase {
				usageBase = runtimePeer.UsedTraffic
			}
			nextUsageBases[key] = usageBase
			previous, seen := baselines[key]
			delta := current
			if seen && current >= previous {
				delta = current - previous
			}
			if delta > 0 {
				totals[wgUsageUID(userID)] += delta
			}
			if runtimePeer.DataLimit != nil && *runtimePeer.DataLimit > 0 && usageBase+current >= *runtimePeer.DataLimit {
				m.removePeer(client, iface, peer.PublicKey)
				m.markPeerLimited(inbound.Tag, key, *runtimePeer.DataLimit)
				delete(next, key)
				delete(nextUsageBases, key)
			}
		}
		m.saveBaselines(iface, next)
		m.saveUsageBases(iface, nextUsageBases)
	}

	out := make([]xray.UserStat, 0, len(totals))
	for uid, value := range totals {
		if value > 0 {
			out = append(out, xray.UserStat{UID: uid, Value: value})
		}
	}
	return out
}

func (m *wgManager) markPeerLimited(inboundTag string, publicKey string, limit int64) {
	if m == nil || limit <= 0 {
		return
	}
	runtimeConfig := m.currentRuntime()
	if runtimeConfig == nil {
		return
	}
	changed := false
	for i := range runtimeConfig.Inbounds {
		if runtimeConfig.Inbounds[i].Tag != inboundTag {
			continue
		}
		for j := range runtimeConfig.Inbounds[i].Peers {
			peer := &runtimeConfig.Inbounds[i].Peers[j]
			if strings.TrimSpace(peer.PublicKey) == publicKey && peer.UsedTraffic < limit {
				peer.UsedTraffic = limit
				changed = true
			}
		}
	}
	if !changed {
		return
	}
	raw, err := json.MarshalIndent(runtimeConfig, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(m.baseDir, "runtime.json"), raw, 0o600)
}

type wgSessionState struct {
	UserID    int64  `json:"user_id"`
	SessionID string `json:"session_id"`
	Address   string `json:"address,omitempty"`
	ClientIP  string `json:"client_ip,omitempty"`
}

func (m *wgManager) syncPeerSessions(client *wgctrl.Client, iface string, inbound wgRuntimeInbound, device *wgtypes.Device, peerByKey map[string]wgRuntimePeer, callback *vpnSessionCallback) {
	if m == nil || client == nil || device == nil {
		return
	}
	now := time.Now()
	previous := m.loadActiveSessions(iface)
	current := map[string]wgSessionState{}
	present := map[string]struct{}{}
	for _, devicePeer := range device.Peers {
		key := devicePeer.PublicKey.String()
		present[key] = struct{}{}
		runtimePeer, ok := peerByKey[key]
		if !ok || !wgPeerActive(runtimePeer) {
			continue
		}
		if devicePeer.LastHandshakeTime.IsZero() || now.Sub(devicePeer.LastHandshakeTime) > wgSessionTimeout {
			continue
		}
		sessionID := "wg:" + iface + ":" + key
		clientIP := wgPeerEndpointIP(devicePeer.Endpoint)
		state := wgSessionState{UserID: runtimePeer.UserID, SessionID: sessionID, Address: runtimePeer.Address, ClientIP: clientIP}
		if _, ok := previous[key]; !ok {
			event := vpnSessionEvent{
				UserID:     runtimePeer.UserID,
				Protocol:   "wg",
				InboundTag: inbound.Tag,
				SessionID:  sessionID,
				AssignedIP: wgPeerAddressHost(runtimePeer.Address),
				ClientIP:   clientIP,
				Event:      "start",
			}
			if !vpnAdmitGoSession(vpnSessionsPath(m.baseDir), callback, event, runtimePeer.DeviceLimit) {
				m.removePeer(client, iface, devicePeer.PublicKey)
				m.deniedUntil[key] = now.Add(wgAdmissionRetryDelay)
				delete(present, key)
				continue
			}
		}
		current[key] = state
	}
	for key, state := range previous {
		if _, ok := current[key]; ok {
			continue
		}
		vpnReleaseGoSession(vpnSessionsPath(m.baseDir), callback, vpnSessionEvent{
			UserID:     state.UserID,
			Protocol:   "wg",
			InboundTag: inbound.Tag,
			SessionID:  state.SessionID,
			AssignedIP: wgPeerAddressHost(state.Address),
			ClientIP:   state.ClientIP,
			Event:      "stop",
		})
	}
	m.restoreAvailablePeers(client, iface, inbound, present)
	m.saveActiveSessions(iface, current)
}

func (m *wgManager) removePeer(client *wgctrl.Client, iface string, key wgtypes.Key) {
	if client == nil || strings.TrimSpace(iface) == "" {
		return
	}
	_ = client.ConfigureDevice(iface, wgtypes.Config{Peers: []wgtypes.PeerConfig{{PublicKey: key, Remove: true}}})
}

func (m *wgManager) restoreAvailablePeers(client *wgctrl.Client, iface string, inbound wgRuntimeInbound, present map[string]struct{}) {
	if client == nil || strings.TrimSpace(iface) == "" {
		return
	}
	sessionsPath := vpnSessionsPath(m.baseDir)
	for _, peer := range inbound.Peers {
		key := strings.TrimSpace(peer.PublicKey)
		if key == "" || !wgPeerActive(peer) {
			continue
		}
		if _, ok := present[key]; ok {
			continue
		}
		if until := m.deniedUntil[key]; until.After(time.Now()) {
			continue
		}
		delete(m.deniedUntil, key)
		if !vpnUserCanOpenSession(sessionsPath, peer.UserID, peer.DeviceLimit) {
			continue
		}
		config, err := wgPeerConfig(peer)
		if err != nil {
			continue
		}
		_ = client.ConfigureDevice(iface, wgtypes.Config{Peers: []wgtypes.PeerConfig{config}})
	}
}

func wgPeerAddressHost(address string) string {
	host := strings.TrimSpace(address)
	if before, _, ok := strings.Cut(host, "/"); ok {
		return strings.TrimSpace(before)
	}
	return host
}

func wgPeerEndpointIP(endpoint *net.UDPAddr) string {
	if endpoint == nil || endpoint.IP == nil {
		return ""
	}
	return endpoint.IP.String()
}

func (m *wgManager) activeSessionsPath(iface string) string {
	return filepath.Join(m.baseDir, iface, "active-sessions.json")
}

func (m *wgManager) loadActiveSessions(iface string) map[string]wgSessionState {
	raw, err := os.ReadFile(m.activeSessionsPath(iface))
	if err != nil {
		return map[string]wgSessionState{}
	}
	out := map[string]wgSessionState{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]wgSessionState{}
	}
	return out
}

func (m *wgManager) saveActiveSessions(iface string, sessions map[string]wgSessionState) {
	dir := filepath.Join(m.baseDir, iface)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	raw, err := json.Marshal(sessions)
	if err != nil {
		return
	}
	_ = os.WriteFile(m.activeSessionsPath(iface), raw, 0o600)
}

func (m *wgManager) applyInbound(client *wgctrl.Client, inbound wgRuntimeInbound) error {
	settings := inbound.Settings
	privateKeyB64 := firstString(settings["private_key"])
	if privateKeyB64 == "" {
		return fmt.Errorf("WireGuard inbound %s requires private_key", inbound.Tag)
	}
	privateKey, err := wgtypes.ParseKey(privateKeyB64)
	if err != nil {
		return fmt.Errorf("WireGuard inbound %s: parse private_key: %w", inbound.Tag, err)
	}
	pool := firstString(settings["address_pool"], settings["ipv4_pool_cidr"], wgDefaultPool)
	serverAddress := firstString(settings["server_address"])
	if serverAddress == "" {
		serverAddress, err = wgServerAddress(pool)
		if err != nil {
			return fmt.Errorf("WireGuard inbound %s: %w", inbound.Tag, err)
		}
	}
	mtu := boundedInt(settings["mtu"], wgDefaultMTU, 576, 1500)
	iface := wgIfaceName(inbound.Tag)
	if err := os.MkdirAll(filepath.Join(m.baseDir, iface), 0o700); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), wgCommandTimeout)
	defer cancel()
	if err := wgEnsureLink(ctx, iface, serverAddress, mtu); err != nil {
		return err
	}

	port := inbound.ListenPort
	deviceConfig := wgtypes.Config{
		PrivateKey:   &privateKey,
		ListenPort:   &port,
		ReplacePeers: false,
	}
	currentPeers := wgCurrentPeers(client, iface)
	desiredKeys := map[string]struct{}{}
	for _, peer := range inbound.Peers {
		if !wgPeerActive(peer) {
			continue
		}
		peerConfig, err := wgPeerConfig(peer)
		if err != nil {
			return fmt.Errorf("WireGuard inbound %s: %w", inbound.Tag, err)
		}
		keyText := strings.TrimSpace(peer.PublicKey)
		peerConfig.ReplaceAllowedIPs = true
		if current, ok := currentPeers[keyText]; ok && current.endpoint != nil {
			peerConfig.Endpoint = current.endpoint
		}
		deviceConfig.Peers = append(deviceConfig.Peers, peerConfig)
		desiredKeys[keyText] = struct{}{}
	}
	for keyText, current := range currentPeers {
		if _, ok := desiredKeys[keyText]; ok {
			continue
		}
		deviceConfig.Peers = append(deviceConfig.Peers, wgtypes.PeerConfig{
			PublicKey: current.publicKey,
			Remove:    true,
		})
	}
	if err := client.ConfigureDevice(iface, deviceConfig); err != nil {
		return fmt.Errorf("configure WireGuard device %s: %w", iface, err)
	}

	if boolValue(settings["tproxy_enabled"], true) {
		if inbound.TunnelPort <= 0 {
			return fmt.Errorf("WireGuard inbound %s requires tunnel_port when tproxy_enabled is true", inbound.Tag)
		}
		if err := wgApplyTProxy(ctx, m.baseDir, inbound, iface); err != nil {
			return fmt.Errorf("WireGuard inbound %s: %w", inbound.Tag, err)
		}
		_ = wgRemoveNAT(ctx, iface)
	} else if boolValue(settings["nat_enabled"], false) {
		enableWGForwarding()
		if err := wgApplyNAT(ctx, iface, wgNetwork(pool)); err != nil {
			return fmt.Errorf("WireGuard inbound %s: %w", inbound.Tag, err)
		}
		_ = wgRemoveTProxy(ctx, iface)
	} else {
		_ = wgRemoveNAT(ctx, iface)
		_ = wgRemoveTProxy(ctx, iface)
	}
	m.pruneBaselines(iface, desiredKeys)
	return nil
}

type wgCurrentPeer struct {
	publicKey wgtypes.Key
	endpoint  *net.UDPAddr
}

func wgCurrentPeers(client *wgctrl.Client, iface string) map[string]wgCurrentPeer {
	out := map[string]wgCurrentPeer{}
	if client == nil || strings.TrimSpace(iface) == "" {
		return out
	}
	device, err := client.Device(iface)
	if err != nil {
		return out
	}
	for _, peer := range device.Peers {
		current := wgCurrentPeer{publicKey: peer.PublicKey}
		if peer.Endpoint == nil {
			out[peer.PublicKey.String()] = current
			continue
		}
		endpoint := *peer.Endpoint
		current.endpoint = &endpoint
		out[peer.PublicKey.String()] = current
	}
	return out
}

func (m *wgManager) currentRuntime() *wgRuntime {
	if m == nil {
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(m.baseDir, "runtime.json"))
	if err != nil {
		return nil
	}
	var payload wgRuntime
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	return &payload
}

// Reconcile re-applies the last persisted WireGuard runtime from disk. Kernel
// WireGuard interfaces (and their NAT rules) do not survive a host reboot, so on
// startup the node must rebuild them from runtime.json. Unlike the Xray config
// cache this runs independently of Xray: WireGuard ingress is kernel-level and
// should come back even if Xray fails to start. Apply is idempotent, so this is
// safe when the interfaces already exist (e.g. a plain service restart).
func (m *wgManager) Reconcile() error {
	if m == nil {
		return nil
	}
	runtimeConfig := m.currentRuntime()
	if runtimeConfig == nil || len(runtimeConfig.Inbounds) == 0 {
		return nil
	}
	return m.Apply(runtimeConfig)
}

func (m *wgManager) pruneRemovedInbounds(desired map[string]struct{}) error {
	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		iface := entry.Name()
		if !strings.HasPrefix(iface, wgIfacePrefix) {
			continue
		}
		if _, ok := desired[iface]; ok {
			continue
		}
		m.teardownInterface(iface)
		_ = os.RemoveAll(filepath.Join(m.baseDir, iface))
	}
	return nil
}

func (m *wgManager) teardownInterface(iface string) {
	if runtime.GOOS == "windows" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), wgCommandTimeout)
	defer cancel()
	_ = wgRemoveNAT(ctx, iface)
	_ = wgRemoveTProxy(ctx, iface)
	_, _ = wgRunIP(ctx, "link", "del", "dev", iface)
}

func (m *wgManager) baselinePath(iface string) string {
	return filepath.Join(m.baseDir, iface, "baselines.json")
}

func (m *wgManager) usageBasePath(iface string) string {
	return filepath.Join(m.baseDir, iface, "usage-bases.json")
}

func (m *wgManager) loadBaselines(iface string) map[string]int64 {
	raw, err := os.ReadFile(m.baselinePath(iface))
	if err != nil {
		return map[string]int64{}
	}
	out := map[string]int64{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]int64{}
	}
	return out
}

func (m *wgManager) saveBaselines(iface string, baselines map[string]int64) {
	dir := filepath.Join(m.baseDir, iface)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	raw, err := json.Marshal(baselines)
	if err != nil {
		return
	}
	_ = os.WriteFile(m.baselinePath(iface), raw, 0o600)
}

func (m *wgManager) loadUsageBases(iface string) map[string]int64 {
	raw, err := os.ReadFile(m.usageBasePath(iface))
	if err != nil {
		return map[string]int64{}
	}
	out := map[string]int64{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]int64{}
	}
	return out
}

func (m *wgManager) saveUsageBases(iface string, baselines map[string]int64) {
	dir := filepath.Join(m.baseDir, iface)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	raw, err := json.Marshal(baselines)
	if err != nil {
		return
	}
	_ = os.WriteFile(m.usageBasePath(iface), raw, 0o600)
}

func (m *wgManager) pruneBaselines(iface string, desiredKeys map[string]struct{}) {
	baselines := m.loadBaselines(iface)
	changed := false
	for key := range baselines {
		if _, ok := desiredKeys[key]; !ok {
			delete(baselines, key)
			changed = true
		}
	}
	if changed {
		m.saveBaselines(iface, baselines)
	}
}

func normalizeWGRuntimeInbound(inbound wgRuntimeInbound) wgRuntimeInbound {
	if inbound.Settings == nil {
		inbound.Settings = map[string]any{}
	}
	if inbound.ListenPort == 0 {
		inbound.ListenPort = intValue(inbound.Settings["listen_port"])
	}
	if inbound.TunnelPort == 0 {
		inbound.TunnelPort = intValue(firstString(inbound.Settings["tunnel_port"], inbound.Settings["xray_tunnel_port"], inbound.Settings["tproxy_port"]))
	}
	return inbound
}

// wgValidateInbounds rejects a runtime whose inbounds collide on the wire: two
// inbounds sharing a UDP listen port would fight over the same socket, and two
// sharing (or overlapping) an address pool would break return routing (the
// kernel installs one route per subnet and only one interface wins the replies).
func wgValidateInbounds(inbounds []wgRuntimeInbound) error {
	usedPorts := map[int]string{}
	var claimed []*net.IPNet
	for _, inbound := range inbounds {
		if strings.TrimSpace(inbound.Tag) == "" {
			return fmt.Errorf("WireGuard inbound tag is required")
		}
		if inbound.ListenPort < 1 || inbound.ListenPort > 65535 {
			return fmt.Errorf("WireGuard inbound %s has invalid listen_port %d", inbound.Tag, inbound.ListenPort)
		}
		if boolValue(inbound.Settings["tproxy_enabled"], true) && inbound.TunnelPort <= 0 {
			return fmt.Errorf("WireGuard inbound %s requires tunnel_port when tproxy_enabled is true", inbound.Tag)
		}
		if other, ok := usedPorts[inbound.ListenPort]; ok {
			return fmt.Errorf("WireGuard inbounds %s and %s share listen_port %d", other, inbound.Tag, inbound.ListenPort)
		}
		usedPorts[inbound.ListenPort] = inbound.Tag

		pool := firstString(inbound.Settings["address_pool"], inbound.Settings["ipv4_pool_cidr"], wgDefaultPool)
		_, network, err := net.ParseCIDR(strings.TrimSpace(pool))
		if err != nil {
			return fmt.Errorf("WireGuard inbound %s has invalid address_pool %q: %w", inbound.Tag, pool, err)
		}
		if network.IP.To4() == nil {
			return fmt.Errorf("WireGuard inbound %s address_pool %s must be IPv4", inbound.Tag, network.String())
		}
		for _, existing := range claimed {
			if network.Contains(existing.IP) || existing.Contains(network.IP) {
				return fmt.Errorf("WireGuard inbound %s pool %s overlaps another inbound's pool %s", inbound.Tag, network.String(), existing.String())
			}
		}
		claimed = append(claimed, network)
		serverAddress := firstString(inbound.Settings["server_address"])
		if serverAddress == "" {
			serverAddress, _ = wgServerAddress(pool)
		}
		if err := wgValidatePeerAddresses(inbound.Tag, pool, serverAddress, inbound.Peers); err != nil {
			return err
		}
	}
	return nil
}

func wgValidatePeerAddresses(tag string, pool string, serverAddress string, peers []wgRuntimePeer) error {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(pool))
	if err != nil {
		return fmt.Errorf("WireGuard inbound %s has invalid address_pool %q: %w", tag, pool, err)
	}
	var server netip.Addr
	if strings.TrimSpace(serverAddress) != "" {
		server, err = wgPeerAddr(serverAddress)
		if err != nil {
			return fmt.Errorf("WireGuard inbound %s has invalid server_address %q: %w", tag, serverAddress, err)
		}
		if !server.Is4() {
			return fmt.Errorf("WireGuard inbound %s server_address %s must be IPv4", tag, server.String())
		}
		if !prefix.Contains(server) {
			return fmt.Errorf("WireGuard inbound %s server_address %s is outside pool %s", tag, server.String(), prefix.String())
		}
	}
	seen := map[netip.Addr]int64{}
	seenKeys := map[string]int64{}
	for _, peer := range peers {
		if peer.UserID <= 0 {
			return fmt.Errorf("WireGuard inbound %s peer has invalid user_id %d", tag, peer.UserID)
		}
		publicKey := strings.TrimSpace(peer.PublicKey)
		if publicKey == "" {
			return fmt.Errorf("WireGuard inbound %s peer %d requires public_key", tag, peer.UserID)
		}
		if _, err := wgtypes.ParseKey(publicKey); err != nil {
			return fmt.Errorf("WireGuard inbound %s peer %d has invalid public_key: %w", tag, peer.UserID, err)
		}
		if other, ok := seenKeys[publicKey]; ok {
			return fmt.Errorf("WireGuard inbound %s peers %d and %d share public_key", tag, other, peer.UserID)
		}
		seenKeys[publicKey] = peer.UserID
		address := strings.TrimSpace(peer.Address)
		if address == "" {
			return fmt.Errorf("WireGuard inbound %s peer %d requires address", tag, peer.UserID)
		}
		addr, err := wgPeerAddr(address)
		if err != nil {
			return fmt.Errorf("WireGuard inbound %s peer %d has invalid address %q: %w", tag, peer.UserID, address, err)
		}
		if !prefix.Contains(addr) {
			return fmt.Errorf("WireGuard inbound %s peer %d address %s is outside pool %s", tag, peer.UserID, addr.String(), prefix.String())
		}
		if addr == prefix.Addr() {
			return fmt.Errorf("WireGuard inbound %s peer %d address %s is the pool network address", tag, peer.UserID, addr.String())
		}
		if server.IsValid() && addr == server {
			return fmt.Errorf("WireGuard inbound %s peer %d address %s is the server address", tag, peer.UserID, addr.String())
		}
		if other, ok := seen[addr]; ok {
			return fmt.Errorf("WireGuard inbound %s peers %d and %d share address %s", tag, other, peer.UserID, addr.String())
		}
		seen[addr] = peer.UserID
	}
	return nil
}

func wgPeerAddr(address string) (netip.Addr, error) {
	address = strings.TrimSpace(address)
	if prefix, err := netip.ParsePrefix(address); err == nil {
		return prefix.Addr(), nil
	}
	return netip.ParseAddr(address)
}

func wgPeerActive(peer wgRuntimePeer) bool {
	if strings.TrimSpace(peer.PublicKey) == "" || strings.TrimSpace(peer.Address) == "" {
		return false
	}
	status := strings.ToLower(strings.TrimSpace(peer.Status))
	if status != "" && status != "active" && status != "on_hold" {
		return false
	}
	if peer.DataLimit != nil && *peer.DataLimit > 0 && peer.UsedTraffic >= *peer.DataLimit {
		return false
	}
	if peer.Expire != nil && *peer.Expire > 0 && time.Now().Unix() >= *peer.Expire {
		return false
	}
	return true
}

func wgPeerConfig(peer wgRuntimePeer) (wgtypes.PeerConfig, error) {
	publicKey, err := wgtypes.ParseKey(strings.TrimSpace(peer.PublicKey))
	if err != nil {
		return wgtypes.PeerConfig{}, fmt.Errorf("parse peer public_key: %w", err)
	}
	allowed, err := wgHostAllowedIPs(peer.Address)
	if err != nil {
		return wgtypes.PeerConfig{}, err
	}
	config := wgtypes.PeerConfig{
		PublicKey:         publicKey,
		ReplaceAllowedIPs: true,
		AllowedIPs:        allowed,
	}
	if psk := strings.TrimSpace(peer.PresharedKey); psk != "" {
		key, err := wgtypes.ParseKey(psk)
		if err != nil {
			return wgtypes.PeerConfig{}, fmt.Errorf("parse peer preshared_key: %w", err)
		}
		config.PresharedKey = &key
	}
	return config, nil
}

// wgHostAllowedIPs pins a peer to exactly its allocated address (/32 for IPv4,
// /128 for IPv6) so it cannot source-spoof another client's tunnel address.
func wgHostAllowedIPs(address string) ([]net.IPNet, error) {
	host := address
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil {
		return nil, fmt.Errorf("invalid peer address %q", address)
	}
	if v4 := ip.To4(); v4 != nil {
		return []net.IPNet{{IP: v4, Mask: net.CIDRMask(32, 32)}}, nil
	}
	return []net.IPNet{{IP: ip, Mask: net.CIDRMask(128, 128)}}, nil
}

// wgServerAddress derives the server's own tunnel address (.1 of the pool) in
// CIDR form when the runtime does not supply one explicitly.
func wgServerAddress(pool string) (string, error) {
	_, network, err := net.ParseCIDR(strings.TrimSpace(pool))
	if err != nil {
		return "", fmt.Errorf("invalid address_pool %q: %w", pool, err)
	}
	ip4 := network.IP.To4()
	if ip4 == nil {
		return "", fmt.Errorf("address_pool %q must be IPv4", pool)
	}
	ones, _ := network.Mask.Size()
	if ones > 30 {
		return "", fmt.Errorf("address_pool %q is too small (need at least /30)", pool)
	}
	server := make(net.IP, 4)
	copy(server, ip4)
	server[3]++
	return fmt.Sprintf("%s/%d", server.String(), ones), nil
}

func wgNetwork(pool string) string {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(pool))
	if err != nil {
		return wgDefaultPool
	}
	return prefix.Masked().String()
}

func wgIfaceName(tag string) string {
	sum := sha1.Sum([]byte(tag))
	return wgIfacePrefix + hex.EncodeToString(sum[:])[:8]
}

func wgUsageUID(userID int64) string {
	return wgUsagePrefix + ":" + strconv.FormatInt(userID, 10)
}

// wgEnsureLink creates the wireguard link if missing, sets its MTU, assigns the
// server address, and brings it up. Every step is idempotent so it is safe on an
// already-configured interface. The link itself is created via iproute2 because
// creating a "wireguard" type link portably from Go still shells out on most
// distros; wgctrl then drives the crypto/peer state over netlink.
func wgEnsureLink(ctx context.Context, iface, serverAddress string, mtu int) error {
	if !wgLinkExists(ctx, iface) {
		if out, err := wgRunIP(ctx, "link", "add", "dev", iface, "type", "wireguard"); err != nil {
			return fmt.Errorf("add wg link %s: %v: %s", iface, err, strings.TrimSpace(out))
		}
	}
	threadedPath := filepath.Join("/sys/class/net", iface, "threaded")
	if err := os.WriteFile(threadedPath, []byte("0"), 0o644); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("disable threaded NAPI on %s: %w", iface, err)
	}
	if mtu > 0 {
		if out, err := wgRunIP(ctx, "link", "set", "dev", iface, "mtu", strconv.Itoa(mtu)); err != nil {
			return fmt.Errorf("set mtu on %s: %v: %s", iface, err, strings.TrimSpace(out))
		}
	}
	if serverAddress != "" && !wgAddressPresent(ctx, iface, serverAddress) {
		if out, err := wgRunIP(ctx, "addr", "add", serverAddress, "dev", iface); err != nil {
			low := strings.ToLower(out)
			if !strings.Contains(low, "file exists") && !strings.Contains(low, "already assigned") {
				return fmt.Errorf("add address %s on %s: %v: %s", serverAddress, iface, err, strings.TrimSpace(out))
			}
		}
	}
	if out, err := wgRunIP(ctx, "link", "set", "up", "dev", iface); err != nil {
		return fmt.Errorf("bring up %s: %v: %s", iface, err, strings.TrimSpace(out))
	}
	return nil
}

func wgLinkExists(ctx context.Context, iface string) bool {
	_, err := wgRunIP(ctx, "link", "show", "dev", iface)
	return err == nil
}

// wgAddressPresent reports whether the host portion of addr is already assigned
// to iface, so address assignment stays idempotent without depending on
// iproute2's version-specific "already exists" wording.
func wgAddressPresent(ctx context.Context, iface, addr string) bool {
	out, err := wgRunIP(ctx, "-o", "addr", "show", "dev", iface)
	if err != nil {
		return false
	}
	host := addr
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	if host == "" {
		return false
	}
	for _, lineText := range strings.Split(out, "\n") {
		idx := strings.Index(lineText, host)
		if idx < 0 {
			continue
		}
		after := lineText[idx+len(host):]
		if after == "" || after[0] < '0' || after[0] > '9' {
			return true
		}
	}
	return false
}

func wgRunIP(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "ip", args...).CombinedOutput()
	return string(out), err
}

func ensureWGPrerequisites() error {
	if runtime.GOOS != "linux" {
		return nil
	}
	missing := missingExecutables("wg", "ip", "nft", "iptables")
	if len(missing) > 0 {
		if err := installWGPackages(); err != nil {
			return err
		}
	}
	for _, executable := range []string{"wg", "ip", "nft", "iptables"} {
		if _, err := exec.LookPath(executable); err != nil {
			return fmt.Errorf("WireGuard prerequisite %s was not found after automatic install", executable)
		}
	}
	if err := loadWGKernelModule(); err != nil {
		return err
	}
	return nil
}

func installWGPackages() error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("WireGuard prerequisites are missing and automatic install requires root")
	}
	switch {
	case commandExists("apt-get"):
		if commandExists("dpkg") {
			_ = runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "dpkg", "--configure", "-a")
		}
		_ = runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "-f", "install", "-y")
		if err := runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "update"); err != nil {
			return err
		}
		if err := runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "install", "-y", "--no-install-recommends", "wireguard-tools", "iproute2", "nftables", "iptables", "kmod"); err != nil {
			if repairErr := runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "-f", "install", "-y"); repairErr != nil {
				return err
			}
			return runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "install", "-y", "--no-install-recommends", "wireguard-tools", "iproute2", "nftables", "iptables", "kmod")
		}
		return nil
	case commandExists("dnf"):
		return runInstallCommand(nil, "dnf", "install", "-y", "wireguard-tools", "iproute", "nftables", "iptables", "kmod")
	case commandExists("yum"):
		return runInstallCommand(nil, "yum", "install", "-y", "wireguard-tools", "iproute", "nftables", "iptables", "kmod")
	case commandExists("apk"):
		return runInstallCommand(nil, "apk", "add", "wireguard-tools", "iproute2", "nftables", "iptables", "kmod")
	default:
		return fmt.Errorf("WireGuard prerequisites are missing and no supported package manager was found")
	}
}

func loadWGKernelModule() error {
	if runtime.GOOS != "linux" {
		return nil
	}
	// The wireguard module is built-in on modern kernels; a failed modprobe there
	// is harmless. Only fail if wgctrl still cannot see the module afterwards.
	_ = exec.Command("modprobe", "wireguard").Run()
	for _, module := range []string{"nf_conntrack", "nf_nat", "iptable_nat", "iptable_filter", "nf_tproxy_ipv4"} {
		_ = exec.Command("modprobe", module).Run()
	}
	client, err := wgctrl.New()
	if err != nil {
		return fmt.Errorf("wireguard kernel support is unavailable: %w", err)
	}
	_ = client.Close()
	return nil
}

// enableWGForwarding turns on IPv4/IPv6 forwarding and relaxes reverse-path
// filtering to "loose", persisting it so it survives a reboot. Without
// forwarding the node routes no client traffic, and strict rp_filter silently
// drops NAT'd return traffic that arrives on a different interface. Best-effort.
func enableWGForwarding() {
	if runtime.GOOS != "linux" {
		return
	}
	_ = os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1"), 0o644)
	_ = os.WriteFile("/proc/sys/net/ipv6/conf/all/forwarding", []byte("1"), 0o644)
	_ = os.WriteFile("/proc/sys/net/ipv4/conf/all/rp_filter", []byte("2"), 0o644)
	_ = os.WriteFile("/proc/sys/net/ipv4/conf/default/rp_filter", []byte("2"), 0o644)
	body := "net.ipv4.ip_forward=1\n" +
		"net.ipv6.conf.all.forwarding=1\n" +
		"net.ipv4.conf.all.rp_filter=2\n" +
		"net.ipv4.conf.default.rp_filter=2\n"
	if err := os.WriteFile("/etc/sysctl.d/99-rebecca-wireguard.conf", []byte(body), 0o644); err == nil {
		_ = exec.Command("sysctl", "--system").Run()
	}
}
