package node

import (
	"bufio"
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
)

type ovRuntime struct {
	GeneratedAt string             `json:"generated_at"`
	Target      string             `json:"target,omitempty"`
	Inbounds    []ovRuntimeInbound `json:"inbounds"`
}

type ovRuntimeInbound struct {
	Tag        string          `json:"tag"`
	TunnelTag  string          `json:"tunnel_tag"`
	Port       int             `json:"port"`
	Transport  string          `json:"transport"`
	TunnelPort int             `json:"tunnel_port"`
	Settings   map[string]any  `json:"settings"`
	Users      []ovRuntimeUser `json:"users"`
}

type ovRuntimeUser struct {
	UserID      int64  `json:"user_id"`
	Username    string `json:"username"`
	VPNUsername string `json:"vpn_username"`
	Password    string `json:"password"`
	IPv4Address string `json:"ipv4_address"`
	Status      string `json:"status"`
	UsedTraffic int64  `json:"used_traffic"`
	DataLimit   *int64 `json:"data_limit,omitempty"`
	Expire      *int64 `json:"expire,omitempty"`
}

type ovManager struct {
	baseDir     string
	installMode string
	mu          sync.Mutex
}

func newOVManager(dataDir string, installMode string) *ovManager {
	return &ovManager{
		baseDir:     filepath.Join(dataDir, "openvpn"),
		installMode: strings.ToLower(strings.TrimSpace(installMode)),
	}
}

func (m *ovManager) Apply(runtimeConfig *ovRuntime) error {
	if m == nil {
		return nil
	}
	if runtimeConfig == nil {
		return nil
	}
	if len(runtimeConfig.Inbounds) > 0 && m.installMode != "binary" {
		return fmt.Errorf("OV is supported only on binary Rebecca-node installs")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(runtimeConfig.Inbounds) > 0 {
		if err := ensureOVPrerequisites(); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(m.baseDir, 0o700); err != nil {
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
		desired[safeName(inbound.Tag)] = struct{}{}
	}
	if err := m.pruneRemovedInbounds(desired); err != nil {
		return err
	}
	for _, inbound := range runtimeConfig.Inbounds {
		name := safeName(inbound.Tag)
		dir := filepath.Join(m.baseDir, name)
		oldConfig, _ := os.ReadFile(filepath.Join(dir, "server.conf"))
		wasRunning, _ := openvpnPIDRunning(filepath.Join(dir, "openvpn.pid"), filepath.Join(dir, "openvpn.log"))
		if err := m.writeInbound(inbound); err != nil {
			return err
		}
		newConfig, _ := os.ReadFile(filepath.Join(dir, "server.conf"))
		restart := !wasRunning || string(oldConfig) != string(newConfig)
		if restart {
			m.stopInboundName(name)
		}
		if err := m.applyInbound(inbound, restart); err != nil {
			return err
		}
	}
	return nil
}

func (m *ovManager) CollectUsage() []xray.UserStat {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	path := filepath.Join(m.baseDir, "usage.tsv")
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	stats := map[string]int64{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "\t")
		if len(parts) < 2 {
			continue
		}
		uid := strings.TrimSpace(parts[0])
		value, _ := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if uid == "" || value <= 0 {
			continue
		}
		stats[uid] += value
	}
	_ = os.WriteFile(path, nil, 0o600)
	out := make([]xray.UserStat, 0, len(stats))
	for uid, value := range stats {
		out = append(out, xray.UserStat{UID: uid, Value: value})
	}
	return out
}

func (m *ovManager) writeInbound(inbound ovRuntimeInbound) error {
	name := safeName(inbound.Tag)
	dir := filepath.Join(m.baseDir, name)
	ccdDir := filepath.Join(dir, "ccd")
	if err := os.MkdirAll(ccdDir, 0o700); err != nil {
		return err
	}
	_, poolMask := ovNetworkMask(firstString(inbound.Settings["ipv4_pool_cidr"], "10.66.0.0/16"))
	usersPath := filepath.Join(dir, "users.tsv")
	if err := os.WriteFile(usersPath, []byte(usersTSV(inbound.Users)), 0o600); err != nil {
		return err
	}
	desiredCCD := map[string]struct{}{}
	for _, user := range inbound.Users {
		if strings.TrimSpace(user.VPNUsername) == "" || strings.TrimSpace(user.IPv4Address) == "" {
			continue
		}
		desiredCCD[safeName(user.VPNUsername)] = struct{}{}
		ccd := fmt.Sprintf("ifconfig-push %s %s\n", user.IPv4Address, poolMask)
		if err := os.WriteFile(filepath.Join(ccdDir, safeName(user.VPNUsername)), []byte(ccd), 0o600); err != nil {
			return err
		}
	}
	if entries, err := os.ReadDir(ccdDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if _, ok := desiredCCD[entry.Name()]; !ok {
				_ = os.Remove(filepath.Join(ccdDir, entry.Name()))
			}
		}
	}
	for path, content := range map[string]string{
		filepath.Join(dir, "auth.sh"):              authScript(usersPath),
		filepath.Join(dir, "client-disconnect.sh"): disconnectScript(usersPath, filepath.Join(m.baseDir, "usage.tsv")),
		filepath.Join(dir, "nftables.nft"):         nftScript(inbound, tunName(inbound.Tag)),
		filepath.Join(dir, "server.conf"):          serverConfig(inbound, dir, ccdDir),
	} {
		mode := os.FileMode(0o600)
		if strings.HasSuffix(path, ".sh") {
			mode = 0o700
		}
		if err := os.WriteFile(path, []byte(content), mode); err != nil {
			return err
		}
	}
	return nil
}

func (m *ovManager) applyInbound(inbound ovRuntimeInbound, restart bool) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	if err := validateServerCredentials(inbound); err != nil {
		return err
	}
	name := safeName(inbound.Tag)
	dir := filepath.Join(m.baseDir, name)
	if inbound.TunnelPort > 0 && boolValue(inbound.Settings["tproxy_enabled"], true) {
		nft, err := exec.LookPath("nft")
		if err != nil {
			return fmt.Errorf("nft executable not found")
		}
		if output, err := exec.Command(nft, "-f", filepath.Join(dir, "nftables.nft")).CombinedOutput(); err != nil {
			return fmt.Errorf("apply OV nftables %s: %v: %s", inbound.Tag, err, strings.TrimSpace(string(output)))
		}
		if err := applyTProxyRouting(); err != nil {
			return err
		}
	}
	if !restart {
		return nil
	}
	openvpn, err := exec.LookPath("openvpn")
	if err != nil {
		return fmt.Errorf("OV executable not found")
	}
	pidPath := filepath.Join(dir, "openvpn.pid")
	_ = os.Remove(filepath.Join(dir, "openvpn.log"))
	cmd := exec.Command(openvpn, "--config", filepath.Join(dir, "server.conf"), "--daemon", "rebecca-openvpn-"+name, "--writepid", pidPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("start OV %s: %v: %s", inbound.Tag, err, startOVErrorDetail(output, filepath.Join(dir, "openvpn.log")))
	}
	time.Sleep(500 * time.Millisecond)
	if running, detail := openvpnPIDRunning(pidPath, filepath.Join(dir, "openvpn.log")); !running {
		return fmt.Errorf("start OV %s: process stopped after launch: %s", inbound.Tag, detail)
	}
	return nil
}

func (m *ovManager) pruneRemovedInbounds(desired map[string]struct{}) error {
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
		name := entry.Name()
		if _, ok := desired[name]; ok {
			continue
		}
		m.stopInboundName(name)
		if err := os.RemoveAll(filepath.Join(m.baseDir, name)); err != nil {
			return err
		}
	}
	return nil
}

func (m *ovManager) stopInboundName(name string) {
	if runtime.GOOS == "windows" {
		return
	}
	dir := filepath.Join(m.baseDir, safeName(name))
	pidPath := filepath.Join(dir, "openvpn.pid")
	if raw, err := os.ReadFile(pidPath); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil && pid > 1 {
			_ = exec.Command("kill", strconv.Itoa(pid)).Run()
		}
		_ = os.Remove(pidPath)
	}
	if nft, err := exec.LookPath("nft"); err == nil {
		_ = exec.Command(nft, "delete", "table", "inet", "rebecca_openvpn_"+safeName(name)).Run()
	}
}

func serverConfig(inbound ovRuntimeInbound, dir string, ccdDir string) string {
	settings := inbound.Settings
	transport := strings.ToLower(firstString(settings["transport"], inbound.Transport, "udp"))
	if transport != "tcp" && transport != "udp" {
		transport = "udp"
	}
	pool := firstString(settings["ipv4_pool_cidr"], "10.66.0.0/16")
	network, mask := ovNetworkMask(pool)
	var b strings.Builder
	line(&b, "port "+strconv.Itoa(inbound.Port))
	line(&b, "proto "+transport)
	line(&b, "dev "+tunName(inbound.Tag))
	line(&b, "dev-type tun")
	line(&b, "topology subnet")
	line(&b, "server "+network+" "+mask)
	line(&b, "client-config-dir "+ccdDir)
	line(&b, "verify-client-cert none")
	line(&b, "username-as-common-name")
	line(&b, "script-security 3")
	line(&b, "auth-user-pass-verify "+filepath.Join(dir, "auth.sh")+" via-env")
	line(&b, "client-disconnect "+filepath.Join(dir, "client-disconnect.sh"))
	line(&b, "keepalive 10 300")
	line(&b, "persist-key")
	line(&b, "persist-tun")
	line(&b, "status "+filepath.Join(dir, "status.log")+" 60")
	line(&b, "log-append "+filepath.Join(dir, "openvpn.log"))
	line(&b, "verb 3")
	if boolValue(settings["redirect_gateway"], true) {
		line(&b, "push \"redirect-gateway def1\"")
	}
	for _, dns := range stringList(settings["dns_servers"]) {
		line(&b, "push \"dhcp-option DNS "+dns+"\"")
	}
	if cipher := firstString(settings["cipher"]); cipher != "" {
		line(&b, "cipher "+cipher)
	}
	if auth := firstString(settings["auth"]); auth != "" {
		line(&b, "auth "+auth)
	}
	if ca := firstString(settings["ca"]); ca != "" {
		inline(&b, "ca", ca)
	}
	if cert := firstString(settings["server_certificate"], settings["serverCertificate"]); cert != "" {
		inline(&b, "cert", cert)
	}
	if key := firstString(settings["server_key"], settings["serverKey"]); key != "" {
		inline(&b, "key", key)
	}
	if dh := firstString(settings["dh"]); dh != "" {
		inline(&b, "dh", dh)
	} else {
		line(&b, "dh none")
	}
	if tlsCrypt := firstString(settings["tls_crypt"]); tlsCrypt != "" {
		inline(&b, "tls-crypt", tlsCrypt)
	}
	if tlsAuth := firstString(settings["tls_auth"]); tlsAuth != "" {
		inline(&b, "tls-auth", tlsAuth)
		line(&b, "key-direction 0")
	}
	if managementPort := intValue(settings["management_port"]); managementPort > 0 {
		line(&b, "management 127.0.0.1 "+strconv.Itoa(managementPort))
	}
	return b.String()
}

func nftScript(inbound ovRuntimeInbound, iface string) string {
	if inbound.TunnelPort <= 0 || !boolValue(inbound.Settings["tproxy_enabled"], true) {
		return ""
	}
	blockedV4, blockedV6 := ovBlockedDestinations()
	var rules strings.Builder
	if len(blockedV4) > 0 {
		line(&rules, fmt.Sprintf(`    iifname "%s" ip daddr { %s } drop`, iface, strings.Join(blockedV4, ", ")))
	}
	if len(blockedV6) > 0 {
		line(&rules, fmt.Sprintf(`    iifname "%s" ip6 daddr { %s } drop`, iface, strings.Join(blockedV6, ", ")))
	}
	return fmt.Sprintf(`table inet rebecca_openvpn_%s {
  chain prerouting {
    type filter hook prerouting priority mangle; policy accept;
%s
    iifname "%s" meta l4proto { tcp, udp } tproxy ip to 127.0.0.1:%d meta mark set 1 accept
  }
}
`, safeName(inbound.Tag), strings.TrimRight(rules.String(), "\n"), iface, inbound.TunnelPort)
}

func ensureOVPrerequisites() error {
	if runtime.GOOS != "linux" {
		return nil
	}
	missing := missingExecutables("openvpn", "nft", "ip")
	if len(missing) > 0 {
		if err := installOVPackages(); err != nil {
			return err
		}
	}
	for _, executable := range []string{"openvpn", "nft", "ip"} {
		if _, err := exec.LookPath(executable); err != nil {
			return fmt.Errorf("OV prerequisite %s was not found after automatic install", executable)
		}
	}
	if modprobe, err := exec.LookPath("modprobe"); err == nil {
		_ = exec.Command(modprobe, "tun").Run()
	}
	if _, err := os.Stat("/dev/net/tun"); err != nil {
		return fmt.Errorf("TUN device is unavailable: %w", err)
	}
	return nil
}

func missingExecutables(names ...string) []string {
	missing := []string{}
	for _, name := range names {
		if _, err := exec.LookPath(name); err != nil {
			missing = append(missing, name)
		}
	}
	return missing
}

func installOVPackages() error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("OV prerequisites are missing and automatic install requires root")
	}
	switch {
	case commandExists("apt-get"):
		if err := runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "update"); err != nil {
			return err
		}
		return runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "install", "-y", "--no-install-recommends", "openvpn", "nftables", "iproute2", "kmod")
	case commandExists("dnf"):
		return runInstallCommand(nil, "dnf", "install", "-y", "openvpn", "nftables", "iproute", "kmod")
	case commandExists("yum"):
		return runInstallCommand(nil, "yum", "install", "-y", "openvpn", "nftables", "iproute", "kmod")
	case commandExists("apk"):
		return runInstallCommand(nil, "apk", "add", "openvpn", "nftables", "iproute2", "kmod")
	default:
		return fmt.Errorf("OV prerequisites are missing and no supported package manager was found")
	}
}

func commandExists(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

func runInstallCommand(env []string, name string, args ...string) error {
	command, err := exec.LookPath(name)
	if err != nil {
		return err
	}
	cmd := exec.Command(command, args...)
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %v: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func ovBlockedDestinations() ([]string, []string) {
	v4 := orderedSet("0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16")
	v6 := orderedSet("::/128", "::1/128", "fc00::/7", "fe80::/10")
	ifaces, err := net.InterfaceAddrs()
	if err == nil {
		for _, item := range ifaces {
			prefix, err := netip.ParsePrefix(item.String())
			if err != nil {
				continue
			}
			addr := prefix.Addr()
			if addr.Is4() {
				v4.add(addr.String())
			} else if addr.Is6() {
				v6.add(addr.String())
			}
		}
	}
	return v4.values, v6.values
}

type stringSet struct {
	seen   map[string]struct{}
	values []string
}

func orderedSet(values ...string) *stringSet {
	set := &stringSet{seen: map[string]struct{}{}}
	for _, value := range values {
		set.add(value)
	}
	return set
}

func (s *stringSet) add(value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	if _, ok := s.seen[value]; ok {
		return
	}
	s.seen[value] = struct{}{}
	s.values = append(s.values, value)
}

func applyTProxyRouting() error {
	if runtime.GOOS == "windows" {
		return nil
	}
	ip, err := exec.LookPath("ip")
	if err != nil {
		return fmt.Errorf("ip executable not found")
	}
	if output, err := exec.Command(ip, "rule", "show").CombinedOutput(); err != nil {
		return fmt.Errorf("%s rule show: %v: %s", ip, err, strings.TrimSpace(string(output)))
	} else if !strings.Contains(string(output), "fwmark 0x1 lookup 100") {
		if err := runCommandIgnoreExists(ip, "rule", "add", "fwmark", "1", "lookup", "100"); err != nil {
			return err
		}
	}
	if output, err := exec.Command(ip, "route", "show", "table", "100").CombinedOutput(); err != nil {
		if isMissingRouteTableOutput(output) {
			if err := runCommandIgnoreExists(ip, "route", "replace", "local", "0.0.0.0/0", "dev", "lo", "table", "100"); err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("%s route show table 100: %v: %s", ip, err, strings.TrimSpace(string(output)))
	} else if strings.TrimSpace(string(output)) != "local default dev lo scope host" {
		if err := runCommandIgnoreExists(ip, "route", "replace", "local", "0.0.0.0/0", "dev", "lo", "table", "100"); err != nil {
			return err
		}
	}
	return nil
}

func isMissingRouteTableOutput(output []byte) bool {
	text := strings.ToLower(string(output))
	return strings.Contains(text, "fib table does not exist") ||
		strings.Contains(text, "dump terminated")
}

func runCommandIgnoreExists(command string, args ...string) error {
	output, err := exec.Command(command, args...).CombinedOutput()
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(string(output)), "file exists") {
		return nil
	}
	return fmt.Errorf("%s %s: %v: %s", command, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
}

func validateServerCredentials(inbound ovRuntimeInbound) error {
	settings := inbound.Settings
	if firstString(settings["ca"]) == "" {
		return fmt.Errorf("OV inbound %s requires ca", inbound.Tag)
	}
	if firstString(settings["server_certificate"], settings["serverCertificate"]) == "" {
		return fmt.Errorf("OV inbound %s requires server_certificate", inbound.Tag)
	}
	if firstString(settings["server_key"], settings["serverKey"]) == "" {
		return fmt.Errorf("OV inbound %s requires server_key", inbound.Tag)
	}
	return nil
}

func authScript(usersPath string) string {
	return fmt.Sprintf(`#!/bin/sh
USERS=%q
now=$(date +%%s)
awk -F '\t' -v u="$username" -v p="$password" -v now="$now" '
  $2 == u && $3 == p && ($7 == "" || $7 == "active" || $7 == "on_hold") {
    if ($6 != "" && $5 >= $6) exit 2
    if ($8 != "" && now >= $8) exit 3
    found=1
  }
  END { exit found ? 0 : 1 }
' "$USERS"
`, usersPath)
}

func disconnectScript(usersPath string, usagePath string) string {
	return fmt.Sprintf(`#!/bin/sh
USERS=%q
USAGE=%q
uid=$(awk -F '\t' -v u="$username" '$2 == u { print $1; exit }' "$USERS")
rx=${bytes_received:-0}
tx=${bytes_sent:-0}
total=$((rx + tx))
if [ -n "$uid" ] && [ "$total" -gt 0 ]; then
  printf 'openvpn:%%s\t%%s\n' "$uid" "$total" >> "$USAGE"
fi
`, usersPath, usagePath)
}

func usersTSV(users []ovRuntimeUser) string {
	var b strings.Builder
	for _, user := range users {
		limit := ""
		if user.DataLimit != nil {
			limit = strconv.FormatInt(*user.DataLimit, 10)
		}
		expire := ""
		if user.Expire != nil {
			expire = strconv.FormatInt(*user.Expire, 10)
		}
		fields := []string{
			strconv.FormatInt(user.UserID, 10),
			user.VPNUsername,
			user.Password,
			user.IPv4Address,
			strconv.FormatInt(user.UsedTraffic, 10),
			limit,
			user.Status,
			expire,
		}
		b.WriteString(strings.Join(fields, "\t"))
		b.WriteByte('\n')
	}
	return b.String()
}

func ovNetworkMask(cidr string) (string, string) {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(cidr))
	if err != nil || !prefix.Addr().Is4() {
		return "10.66.0.0", "255.255.0.0"
	}
	bits := prefix.Bits()
	mask := netip.PrefixFrom(netip.AddrFrom4([4]byte{255, 255, 255, 255}), bits).Masked().Addr().As4()
	return prefix.Masked().Addr().String(), fmt.Sprintf("%d.%d.%d.%d", mask[0], mask[1], mask[2], mask[3])
}

func tunName(tag string) string {
	sum := sha1.Sum([]byte(tag))
	return "rbov" + hex.EncodeToString(sum[:])[:8]
}

func safeName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "openvpn"
	}
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "openvpn"
	}
	return out
}

func line(b *strings.Builder, value string) {
	b.WriteString(value)
	b.WriteByte('\n')
}

func inline(b *strings.Builder, name string, content string) {
	line(b, "<"+name+">")
	b.WriteString(strings.TrimSpace(content))
	b.WriteByte('\n')
	line(b, "</"+name+">")
}

func startOVErrorDetail(output []byte, logPath string) string {
	parts := []string{}
	if text := strings.TrimSpace(string(output)); text != "" {
		parts = append(parts, text)
	}
	if text := tailOVFile(logPath, 4096); text != "" {
		parts = append(parts, text)
	}
	if len(parts) == 0 {
		return "no output"
	}
	return strings.Join(parts, "\n")
}

func openvpnPIDRunning(pidPath string, logPath string) (bool, string) {
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		return false, startOVErrorDetail(nil, logPath)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 1 {
		return false, startOVErrorDetail(nil, logPath)
	}
	if err := exec.Command("kill", "-0", strconv.Itoa(pid)).Run(); err != nil {
		return false, startOVErrorDetail(nil, logPath)
	}
	return true, ""
}

func tailOVFile(path string, limit int) string {
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) == 0 {
		return ""
	}
	if limit > 0 && len(raw) > limit {
		raw = raw[len(raw)-limit:]
	}
	return strings.TrimSpace(string(raw))
}

func firstString(values ...any) string {
	for _, value := range values {
		text := strings.TrimSpace(fmt.Sprint(value))
		if text != "" && text != "<nil>" {
			return text
		}
	}
	return ""
}

func stringList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := firstString(item); text != "" {
				out = append(out, text)
			}
		}
		return out
	case string:
		parts := strings.FieldsFunc(typed, func(r rune) bool {
			return r == ',' || r == '\n' || r == '\r'
		})
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			if text := strings.TrimSpace(part); text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		if text := firstString(value); text != "" {
			return []string{text}
		}
		return nil
	}
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case string:
		parsed, _ := strconv.Atoi(strings.TrimSpace(typed))
		return parsed
	default:
		return 0
	}
}

func boolValue(value any, fallback bool) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	}
	return fallback
}
