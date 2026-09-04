//go:build linux

package node

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
	"golang.org/x/sys/unix"
)

const greSessionTimeout = 3 * time.Minute

type greDynamicPeer struct {
	Iface        string
	Inner        string
	UserID       int64
	DeviceLimit  int64
	InboundTag   string
	Callback     *vpnSessionCallback
	SessionsPath string
}

type greBinding struct {
	Outer    string
	LastSeen time.Time
	Peer     greDynamicPeer
}

type greLearner struct {
	mu      sync.Mutex
	peers   map[string]greDynamicPeer
	bound   map[string]greBinding
	stop    chan struct{}
	wake    chan struct{}
	started bool
}

func newGRELearner() *greLearner {
	return &greLearner{peers: map[string]greDynamicPeer{}, bound: map[string]greBinding{}, stop: make(chan struct{}), wake: make(chan struct{}, 1)}
}

func (m *extraVPNManager) applyGRELocked(inbounds []extraRuntimeInbound, callback *vpnSessionCallback) error {
	previous := []string{}
	statePath := filepath.Join(m.baseDir, "gre-interfaces.json")
	if raw, err := os.ReadFile(statePath); err == nil {
		_ = json.Unmarshal(raw, &previous)
	}
	for _, inbound := range filterExtraVPNInbounds(runtimeInbounds(m.runtime), "gre") {
		_ = removeExtraVPNNetworking("gre_" + safeName(inbound.Tag))
	}
	if len(inbounds) == 0 {
		m.greLearner.SetPeers(nil)
		for _, iface := range previous {
			_ = exec.Command("ip", "link", "del", "dev", iface).Run()
		}
		_ = os.Remove(statePath)
		return nil
	}
	if err := ensureGREPrerequisites(); err != nil {
		return err
	}
	groups := map[string][]extraRuntimeInbound{}
	for _, inbound := range inbounds {
		local := stringSetting(inbound.Settings, "local_address")
		if local == "" {
			local = defaultRouteSource()
		}
		if net.ParseIP(local) == nil {
			return fmt.Errorf("GRE inbound %s has no usable local IPv4 address", inbound.Tag)
		}
		groups[local] = append(groups[local], inbound)
	}
	desiredIfaces := []string{}
	peers := map[string]greDynamicPeer{}
	for local, group := range groups {
		iface := greIfaceName(local)
		desiredIfaces = append(desiredIfaces, iface)
		ttl := intSetting(group[0].Settings, "ttl", 64)
		if !wgLinkExists(context.Background(), iface) {
			if output, err := exec.Command("ip", "tunnel", "add", iface, "mode", "gre", "local", local, "ttl", strconv.Itoa(ttl)).CombinedOutput(); err != nil {
				return fmt.Errorf("create GRE interface %s: %v: %s", iface, err, strings.TrimSpace(string(output)))
			}
		} else if output, err := exec.Command("ip", "tunnel", "change", iface, "mode", "gre", "local", local, "ttl", strconv.Itoa(ttl)).CombinedOutput(); err != nil {
			return fmt.Errorf("update GRE interface %s: %v: %s", iface, err, strings.TrimSpace(string(output)))
		}
		mtu := 1476
		for _, inbound := range group {
			if value := intSetting(inbound.Settings, "mtu", 1476); value < mtu {
				mtu = value
			}
		}
		if output, err := exec.Command("ip", "link", "set", "dev", iface, "mtu", strconv.Itoa(mtu), "up").CombinedOutput(); err != nil {
			return fmt.Errorf("bring GRE interface up: %v: %s", err, strings.TrimSpace(string(output)))
		}
		for _, inbound := range group {
			pool := firstString(inbound.Settings["ipv4_pool_cidr"], "10.74.0.0/16")
			serverAddress, err := wgServerAddress(pool)
			if err != nil {
				return err
			}
			if output, err := exec.Command("ip", "addr", "replace", serverAddress, "dev", iface).CombinedOutput(); err != nil {
				return fmt.Errorf("configure GRE address: %v: %s", err, strings.TrimSpace(string(output)))
			}
			if output, err := exec.Command("ip", "route", "replace", pool, "dev", iface).CombinedOutput(); err != nil {
				return fmt.Errorf("configure GRE route: %v: %s", err, strings.TrimSpace(string(output)))
			}
			converted := remoteAccessRuntimeInbound{Tag: inbound.Tag, TunnelTag: inbound.TunnelTag, Port: 47, TunnelPort: inbound.TunnelPort, Settings: inbound.Settings}
			networkIface := ""
			if inbound.TunnelPort <= 0 || !boolValue(inbound.Settings["tproxy_enabled"], true) {
				networkIface = iface
			}
			if err := applyRemoteAccessNetworking("gre_"+safeName(inbound.Tag), networkIface, converted); err != nil {
				return err
			}
			for _, user := range inbound.Users {
				if !extraVPNUserActive(user) {
					continue
				}
				addresses := user.IPv4Addresses
				if len(addresses) == 0 && user.IPv4Address != "" {
					addresses = []string{user.IPv4Address}
				}
				for _, address := range addresses {
					inner := wgPeerAddressHost(address)
					if net.ParseIP(inner) == nil {
						continue
					}
					peers[inner] = greDynamicPeer{Iface: iface, Inner: inner, UserID: user.UserID, DeviceLimit: user.DeviceLimit, InboundTag: inbound.Tag, Callback: callback, SessionsPath: vpnSessionsPath(m.baseDir)}
				}
			}
		}
	}
	for _, iface := range previous {
		if !containsString(desiredIfaces, iface) {
			_ = exec.Command("ip", "link", "del", "dev", iface).Run()
		}
	}
	if err := applyGREGuard(peers); err != nil {
		return err
	}
	m.greLearner.SetPeers(peers)
	raw, _ := json.Marshal(desiredIfaces)
	return writeFileAtomic(statePath, raw, 0o600)
}

func runtimeInbounds(runtimeConfig *extraRuntime) []extraRuntimeInbound {
	if runtimeConfig == nil {
		return nil
	}
	return runtimeConfig.Inbounds
}

func ensureGREPrerequisites() error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("GRE requires root privileges")
	}
	if !commandExists("ip") || !commandExists("nft") || !commandExists("iptables") {
		if !commandExists("apt-get") {
			return fmt.Errorf("GRE requires iproute2, nftables and iptables")
		}
		if err := runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "update"); err != nil {
			return err
		}
		if err := runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "install", "-y", "--no-install-recommends", "iproute2", "nftables", "iptables", "kmod"); err != nil {
			return err
		}
	}
	if output, err := exec.Command("modprobe", "ip_gre").CombinedOutput(); err != nil {
		return fmt.Errorf("load ip_gre: %v: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func defaultRouteSource() string {
	output, _ := exec.Command("ip", "-4", "route", "get", "1.1.1.1").Output()
	fields := strings.Fields(string(output))
	for index := 0; index+1 < len(fields); index++ {
		if fields[index] == "src" {
			return fields[index+1]
		}
	}
	return ""
}

func greIfaceName(local string) string {
	sum := sha1.Sum([]byte(local))
	return "rbgre" + hex.EncodeToString(sum[:])[:8]
}

func applyGREGuard(peers map[string]greDynamicPeer) error {
	_ = exec.Command("nft", "delete", "table", "inet", "rebecca_gre_guard").Run()
	if len(peers) == 0 {
		return nil
	}
	byIface := map[string][]string{}
	for inner, peer := range peers {
		byIface[peer.Iface] = append(byIface[peer.Iface], inner)
	}
	var rules strings.Builder
	rules.WriteString("table inet rebecca_gre_guard {\n chain prerouting { type filter hook prerouting priority -151; policy accept;\n")
	for iface, addresses := range byIface {
		fmt.Fprintf(&rules, "  iifname %q ip saddr { %s } accept\n  iifname %q drop\n", iface, strings.Join(addresses, ", "), iface)
	}
	rules.WriteString(" }\n}\n")
	command := exec.Command("nft", "-f", "-")
	command.Stdin = strings.NewReader(rules.String())
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("apply GRE source guard: %v: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func removeExtraVPNNetworking(name string) error {
	_ = exec.Command("nft", "delete", "table", "inet", "rebecca_"+safeName(name)).Run()
	return vpnRemoveDirectNAT(name)
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (l *greLearner) SetPeers(peers map[string]greDynamicPeer) {
	if peers == nil {
		peers = map[string]greDynamicPeer{}
	}
	l.mu.Lock()
	for inner, binding := range l.bound {
		if _, ok := peers[inner]; !ok {
			l.releaseLocked(inner, binding)
			delete(l.bound, inner)
		}
	}
	l.peers = peers
	start := !l.started && len(peers) > 0
	if start {
		l.started = true
	}
	l.mu.Unlock()
	if start {
		go l.run()
	}
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *greLearner) run() {
	for {
		l.mu.Lock()
		if len(l.peers) == 0 {
			l.started = false
			l.mu.Unlock()
			return
		}
		l.expireLocked()
		l.mu.Unlock()
		l.sniff(5 * time.Second)
		select {
		case <-l.stop:
			return
		case <-l.wake:
		case <-time.After(5 * time.Second):
		}
	}
}

func (l *greLearner) sniff(window time.Duration) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_RAW, unix.IPPROTO_GRE)
	if err != nil {
		return
	}
	defer unix.Close(fd)
	_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, 64*1024)
	_ = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 1})
	buf := make([]byte, 2048)
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		n, _, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK || err == unix.EINTR {
				continue
			}
			return
		}
		outer, inner, ok := parseGREPair(buf[:n])
		if ok {
			l.bind(inner, outer)
		}
	}
}

func (l *greLearner) bind(inner, outer string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	peer, ok := l.peers[inner]
	if !ok {
		return
	}
	if previous, exists := l.bound[inner]; exists {
		if previous.Outer == outer {
			previous.LastSeen = time.Now()
			l.bound[inner] = previous
		}
		return
	}
	event := vpnSessionEvent{UserID: peer.UserID, Protocol: "gre", InboundTag: peer.InboundTag, SessionID: "gre:" + peer.Iface + ":" + inner + ":" + outer, AssignedIP: inner, ClientIP: outer, Event: "start"}
	if !vpnAdmitGoSession(peer.SessionsPath, peer.Callback, event, peer.DeviceLimit) {
		return
	}
	if output, err := exec.Command("ip", "neigh", "replace", inner, "lladdr", outer, "nud", "permanent", "dev", peer.Iface).CombinedOutput(); err != nil {
		vpnReleaseGoSession(peer.SessionsPath, peer.Callback, event)
		_ = output
		return
	}
	l.bound[inner] = greBinding{Outer: outer, LastSeen: time.Now(), Peer: peer}
}

func (l *greLearner) expireLocked() {
	for inner, binding := range l.bound {
		if time.Since(binding.LastSeen) > greSessionTimeout {
			l.releaseLocked(inner, binding)
			delete(l.bound, inner)
		}
	}
}

func (l *greLearner) releaseLocked(inner string, binding greBinding) {
	_ = exec.Command("ip", "neigh", "del", inner, "dev", binding.Peer.Iface).Run()
	vpnReleaseGoSession(binding.Peer.SessionsPath, binding.Peer.Callback, vpnSessionEvent{UserID: binding.Peer.UserID, Protocol: "gre", InboundTag: binding.Peer.InboundTag, SessionID: "gre:" + binding.Peer.Iface + ":" + inner + ":" + binding.Outer, AssignedIP: inner, ClientIP: binding.Outer, Event: "stop"})
}

func parseGREPair(packet []byte) (outer, inner string, ok bool) {
	if len(packet) < 24 || packet[0]>>4 != 4 || packet[9] != unix.IPPROTO_GRE {
		return "", "", false
	}
	ihl := int(packet[0]&0x0f) * 4
	if ihl < 20 || len(packet) < ihl+4 {
		return "", "", false
	}
	gre := packet[ihl:]
	flags := uint16(gre[0])<<8 | uint16(gre[1])
	if uint16(gre[2])<<8|uint16(gre[3]) != 0x0800 {
		return "", "", false
	}
	offset := 4
	if flags&0x8000 != 0 {
		offset += 4
	}
	if flags&0x2000 != 0 {
		offset += 4
	}
	if flags&0x1000 != 0 {
		offset += 4
	}
	if len(gre) < offset+20 || gre[offset]>>4 != 4 {
		return "", "", false
	}
	return net.IP(packet[12:16]).String(), net.IP(gre[offset+12 : offset+16]).String(), true
}

func (m *extraVPNManager) collectGREUsageLocked() []xray.UserStat { return nil }

func runningGREInbounds(runtimeConfig *extraRuntime) int {
	if runtimeConfig == nil {
		return 0
	}
	running := 0
	for _, inbound := range filterExtraVPNInbounds(runtimeConfig.Inbounds, "gre") {
		local := stringSetting(inbound.Settings, "local_address")
		if local == "" {
			local = defaultRouteSource()
		}
		if local != "" && wgLinkExists(context.Background(), greIfaceName(local)) {
			running++
		}
	}
	return running
}
