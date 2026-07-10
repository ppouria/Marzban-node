package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	sourceIPBlockNFTTable         = "rebecca_xray_limiter"
	defaultSourceIPBlockTTLSecond = uint32(30 * 60)
	maxSourceIPBlockTTLSecond     = uint32(24 * 60 * 60)
)

type sourceIPBlocker struct {
	baseDir     string
	installMode string
	mu          sync.Mutex
}

type sourceIPBlockEntry struct {
	IP         string
	TTLSeconds uint32
	UserUID    string
	Reason     string
}

type sourceIPBlockPorts struct {
	TCP []uint32
	UDP []uint32
}

func newSourceIPBlocker(dataDir, installMode string) *sourceIPBlocker {
	return &sourceIPBlocker{
		baseDir:     filepath.Join(dataDir, "source-ip-blocks"),
		installMode: strings.ToLower(strings.TrimSpace(installMode)),
	}
}

func (b *sourceIPBlocker) Apply(ctx context.Context, entries []sourceIPBlockEntry, ports sourceIPBlockPorts, protectedIPs []string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	normalized, err := normalizeSourceIPBlockEntries(entries, protectedIPs)
	if err != nil {
		return err
	}
	if len(normalized) == 0 {
		return b.clearLocked(ctx)
	}
	if runtime.GOOS != "linux" {
		return errors.New("source IP blocking is supported only on Linux")
	}
	if b.installMode != "binary" {
		return errors.New("source IP blocking is supported only for binary node installs")
	}
	ports, err = normalizeSourceIPBlockPorts(ports)
	if err != nil {
		return err
	}
	if len(ports.TCP) == 0 && len(ports.UDP) == 0 {
		return errors.New("at least one Xray inbound port is required for source IP blocking")
	}
	if err := ensureSourceIPBlockPrerequisites(); err != nil {
		return err
	}
	script := buildSourceIPBlockNFTScript(normalized, ports)
	if err := os.MkdirAll(b.baseDir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(b.baseDir, "nftables.nft")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		return err
	}
	if err := b.deleteTableLocked(ctx); err != nil {
		return err
	}
	return b.applyScriptLocked(ctx, path)
}

func (b *sourceIPBlocker) Clear(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.clearLocked(ctx)
}

func (b *sourceIPBlocker) clearLocked(ctx context.Context) error {
	if runtime.GOOS != "linux" {
		return nil
	}
	return b.deleteTableLocked(ctx)
}

func (b *sourceIPBlocker) deleteTableLocked(ctx context.Context) error {
	nft, err := exec.LookPath("nft")
	if err != nil {
		return nil
	}
	_ = exec.CommandContext(ctx, nft, "delete", "table", "inet", sourceIPBlockNFTTable).Run()
	return nil
}

func (b *sourceIPBlocker) applyScriptLocked(ctx context.Context, path string) error {
	nft, err := exec.LookPath("nft")
	if err != nil {
		return err
	}
	output, err := exec.CommandContext(ctx, nft, "-f", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("nft apply source IP blocks: %v: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func ensureSourceIPBlockPrerequisites() error {
	if runtime.GOOS != "linux" {
		return nil
	}
	if len(missingExecutables("nft")) > 0 {
		if err := installSourceIPBlockPackages(); err != nil {
			return err
		}
	}
	if _, err := exec.LookPath("nft"); err != nil {
		return fmt.Errorf("source IP blocking prerequisite nft was not found after automatic install")
	}
	return nil
}

func installSourceIPBlockPackages() error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("source IP blocking prerequisites are missing and automatic install requires root")
	}
	switch {
	case commandExists("apt-get"):
		if err := runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "update"); err != nil {
			return err
		}
		return runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "install", "-y", "--no-install-recommends", "nftables")
	case commandExists("dnf"):
		return runInstallCommand(nil, "dnf", "install", "-y", "nftables")
	case commandExists("yum"):
		return runInstallCommand(nil, "yum", "install", "-y", "nftables")
	case commandExists("apk"):
		return runInstallCommand(nil, "apk", "add", "nftables")
	default:
		return fmt.Errorf("source IP blocking prerequisites are missing and no supported package manager was found")
	}
}

func normalizeSourceIPBlockEntries(entries []sourceIPBlockEntry, protectedIPs []string) ([]sourceIPBlockEntry, error) {
	protected := map[netip.Addr]struct{}{}
	for _, raw := range protectedIPs {
		addr, err := netip.ParseAddr(strings.TrimSpace(raw))
		if err == nil {
			protected[addr] = struct{}{}
		}
	}
	byAddr := map[netip.Addr]sourceIPBlockEntry{}
	for _, entry := range entries {
		addr, err := netip.ParseAddr(strings.TrimSpace(entry.IP))
		if err != nil {
			return nil, fmt.Errorf("invalid source IP block address %q", entry.IP)
		}
		if addr.IsLoopback() || addr.IsUnspecified() || addr.IsMulticast() {
			continue
		}
		if _, ok := protected[addr]; ok {
			continue
		}
		entry.IP = addr.String()
		entry.TTLSeconds = normalizedSourceIPBlockTTL(entry.TTLSeconds)
		if current, ok := byAddr[addr]; ok && current.TTLSeconds >= entry.TTLSeconds {
			continue
		}
		byAddr[addr] = entry
	}
	result := make([]sourceIPBlockEntry, 0, len(byAddr))
	for _, entry := range byAddr {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].IP < result[j].IP
	})
	return result, nil
}

func normalizedSourceIPBlockTTL(value uint32) uint32 {
	if value == 0 {
		return defaultSourceIPBlockTTLSecond
	}
	if value > maxSourceIPBlockTTLSecond {
		return maxSourceIPBlockTTLSecond
	}
	return value
}

func normalizeSourceIPBlockPorts(ports sourceIPBlockPorts) (sourceIPBlockPorts, error) {
	tcp, err := normalizeSourceIPBlockPortList(ports.TCP)
	if err != nil {
		return sourceIPBlockPorts{}, err
	}
	udp, err := normalizeSourceIPBlockPortList(ports.UDP)
	if err != nil {
		return sourceIPBlockPorts{}, err
	}
	return sourceIPBlockPorts{TCP: tcp, UDP: udp}, nil
}

func normalizeSourceIPBlockPortList(values []uint32) ([]uint32, error) {
	seen := map[uint32]struct{}{}
	for _, value := range values {
		if value == 0 || value > 65535 {
			return nil, fmt.Errorf("invalid source IP block port %d", value)
		}
		seen[value] = struct{}{}
	}
	result := make([]uint32, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result, nil
}

func buildSourceIPBlockNFTScript(entries []sourceIPBlockEntry, ports sourceIPBlockPorts) string {
	v4 := sourceIPBlockNFTElements(entries, true)
	v6 := sourceIPBlockNFTElements(entries, false)

	var b strings.Builder
	line(&b, fmt.Sprintf("table inet %s {", sourceIPBlockNFTTable))
	writeSourceIPBlockSet(&b, "blocked4", "ipv4_addr", v4)
	writeSourceIPBlockSet(&b, "blocked6", "ipv6_addr", v6)
	line(&b, "  chain input {")
	line(&b, "    type filter hook input priority 0; policy accept;")
	if len(v4) > 0 && len(ports.TCP) > 0 {
		line(&b, fmt.Sprintf("    ip saddr @blocked4 tcp dport { %s } drop", sourceIPBlockPortSet(ports.TCP)))
	}
	if len(v4) > 0 && len(ports.UDP) > 0 {
		line(&b, fmt.Sprintf("    ip saddr @blocked4 udp dport { %s } drop", sourceIPBlockPortSet(ports.UDP)))
	}
	if len(v6) > 0 && len(ports.TCP) > 0 {
		line(&b, fmt.Sprintf("    ip6 saddr @blocked6 tcp dport { %s } drop", sourceIPBlockPortSet(ports.TCP)))
	}
	if len(v6) > 0 && len(ports.UDP) > 0 {
		line(&b, fmt.Sprintf("    ip6 saddr @blocked6 udp dport { %s } drop", sourceIPBlockPortSet(ports.UDP)))
	}
	line(&b, "  }")
	line(&b, "}")
	return b.String()
}

func writeSourceIPBlockSet(b *strings.Builder, name, typ string, elements []string) {
	line(b, fmt.Sprintf("  set %s {", name))
	line(b, fmt.Sprintf("    type %s", typ))
	line(b, "    flags timeout")
	if len(elements) > 0 {
		line(b, fmt.Sprintf("    elements = { %s }", strings.Join(elements, ", ")))
	}
	line(b, "  }")
}

func sourceIPBlockNFTElements(entries []sourceIPBlockEntry, wantV4 bool) []string {
	elements := make([]string, 0, len(entries))
	for _, entry := range entries {
		addr, err := netip.ParseAddr(entry.IP)
		if err != nil || addr.Is4() != wantV4 {
			continue
		}
		elements = append(elements, fmt.Sprintf("%s timeout %ds", addr.String(), entry.TTLSeconds))
	}
	return elements
}

func sourceIPBlockPortSet(ports []uint32) string {
	values := make([]string, 0, len(ports))
	for _, port := range ports {
		values = append(values, strconv.FormatUint(uint64(port), 10))
	}
	return strings.Join(values, ", ")
}

func (s *Server) sourceIPBlockPorts(tcpPorts, udpPorts []uint32) (sourceIPBlockPorts, error) {
	ports, err := normalizeSourceIPBlockPorts(sourceIPBlockPorts{TCP: tcpPorts, UDP: udpPorts})
	if err != nil {
		return sourceIPBlockPorts{}, err
	}
	if len(ports.TCP) > 0 || len(ports.UDP) > 0 {
		return ports, nil
	}

	s.mu.Lock()
	cfg := s.lastConfig
	s.mu.Unlock()
	if cfg == nil {
		return sourceIPBlockPorts{}, errors.New("xray runtime config is unavailable and no source IP block ports were provided")
	}
	raw, err := cfg.JSON()
	if err != nil {
		return sourceIPBlockPorts{}, err
	}
	ports, err = sourceIPBlockPortsFromConfig(raw)
	if err != nil {
		return sourceIPBlockPorts{}, err
	}
	if len(ports.TCP) == 0 && len(ports.UDP) == 0 {
		return sourceIPBlockPorts{}, errors.New("no Xray inbound ports were found for source IP blocking")
	}
	return ports, nil
}

func (s *Server) protectedSourceIPs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(s.clientIP) == "" {
		return nil
	}
	return []string{s.clientIP}
}

func sourceIPBlockPortsFromConfig(raw []byte) (sourceIPBlockPorts, error) {
	var cfg struct {
		Inbounds []map[string]any `json:"inbounds"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return sourceIPBlockPorts{}, err
	}
	ports := sourceIPBlockPorts{}
	for _, inbound := range cfg.Inbounds {
		tag := strings.TrimSpace(sourceIPBlockString(inbound["tag"]))
		protocol := strings.ToLower(strings.TrimSpace(sourceIPBlockString(inbound["protocol"])))
		if tag == "API_INBOUND" || protocol == "api" || protocol == "tunnel" || protocol == "dokodemo-door" {
			continue
		}
		port, ok := sourceIPBlockSinglePort(inbound["port"])
		if !ok {
			continue
		}
		for _, network := range sourceIPBlockInboundNetworks(inbound) {
			if sourceIPBlockNetworkIsUDP(network) {
				ports.UDP = append(ports.UDP, port)
			} else {
				ports.TCP = append(ports.TCP, port)
			}
		}
	}
	return normalizeSourceIPBlockPorts(ports)
}

func sourceIPBlockInboundNetworks(inbound map[string]any) []string {
	network := sourceIPBlockNestedString(inbound, "streamSettings", "network")
	if network == "" {
		network = sourceIPBlockNestedString(inbound, "settings", "network")
	}
	if network == "" {
		return []string{"tcp"}
	}
	parts := strings.FieldsFunc(strings.ToLower(network), func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	if len(result) == 0 {
		return []string{"tcp"}
	}
	return result
}

func sourceIPBlockNetworkIsUDP(network string) bool {
	switch strings.ToLower(strings.TrimSpace(network)) {
	case "udp", "kcp", "quic":
		return true
	default:
		return false
	}
}

func sourceIPBlockNestedString(parent map[string]any, key, child string) string {
	value, ok := parent[key].(map[string]any)
	if !ok {
		return ""
	}
	return sourceIPBlockString(value[child])
}

func sourceIPBlockString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func sourceIPBlockSinglePort(value any) (uint32, bool) {
	switch typed := value.(type) {
	case float64:
		if typed <= 0 || typed > 65535 || typed != float64(uint32(typed)) {
			return 0, false
		}
		return uint32(typed), true
	case int:
		if typed <= 0 || typed > 65535 {
			return 0, false
		}
		return uint32(typed), true
	case json.Number:
		parsed, err := strconv.ParseUint(typed.String(), 10, 16)
		if err != nil || parsed == 0 {
			return 0, false
		}
		return uint32(parsed), true
	case string:
		text := strings.TrimSpace(typed)
		if text == "" || strings.Contains(text, "-") {
			return 0, false
		}
		parsed, err := strconv.ParseUint(text, 10, 16)
		if err != nil || parsed == 0 {
			return 0, false
		}
		return uint32(parsed), true
	default:
		return 0, false
	}
}
