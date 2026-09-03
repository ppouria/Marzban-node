package node

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

type remoteAccessRuntime struct {
	GeneratedAt     string                       `json:"generated_at"`
	Target          string                       `json:"target,omitempty"`
	SessionCallback *vpnSessionCallback          `json:"session_callback,omitempty"`
	Inbounds        []remoteAccessRuntimeInbound `json:"inbounds"`
}

type remoteAccessRuntimeInbound struct {
	Tag        string                    `json:"tag"`
	TunnelTag  string                    `json:"tunnel_tag"`
	Port       int                       `json:"port"`
	TunnelPort int                       `json:"tunnel_port"`
	Settings   map[string]any            `json:"settings"`
	Users      []remoteAccessRuntimeUser `json:"users"`
}

type remoteAccessRuntimeUser struct {
	UserID      int64  `json:"user_id"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	IPv4Address string `json:"ipv4_address"`
	Status      string `json:"status"`
	UsedTraffic int64  `json:"used_traffic"`
	DataLimit   *int64 `json:"data_limit,omitempty"`
	Expire      *int64 `json:"expire,omitempty"`
	DeviceLimit int64  `json:"device_limit,omitempty"`
}

type remoteAccessManager struct {
	baseDir     string
	installMode string
	mu          sync.Mutex
}

func (m *remoteAccessManager) sessionsPath() string {
	return filepath.Join(filepath.Dir(m.baseDir), "vpn-sessions.tsv")
}

func newRemoteAccessManager(dataDir, installMode string) *remoteAccessManager {
	return &remoteAccessManager{baseDir: filepath.Join(dataDir, "remote-access"), installMode: strings.ToLower(strings.TrimSpace(installMode))}
}

func (m *remoteAccessManager) ApplyIKEv2(config *remoteAccessRuntime) error {
	return m.apply("ikev2", config)
}

func (m *remoteAccessManager) ApplyAnyConnect(config *remoteAccessRuntime) error {
	return m.apply("anyconnect", config)
}

func (m *remoteAccessManager) apply(protocol string, config *remoteAccessRuntime) error {
	if m == nil || config == nil {
		return nil
	}
	if len(config.Inbounds) > 0 && m.installMode != "binary" {
		return fmt.Errorf("%s is supported only on binary Rebecca-node installs", protocol)
	}
	if protocol == "ikev2" && len(config.Inbounds) > 1 {
		return fmt.Errorf("only one IKEv2 inbound is supported per node because UDP 500/4500 are node-wide")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	dir := filepath.Join(m.baseDir, protocol)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileIfChanged(filepath.Join(dir, "runtime.json"), raw, 0o600); err != nil {
		return err
	}
	if len(config.Inbounds) == 0 {
		return m.stop(protocol)
	}
	if protocol == "ikev2" {
		return m.applyIKEv2(config.Inbounds[0], config.SessionCallback)
	}
	return m.applyAnyConnect(config.Inbounds, config.SessionCallback)
}

func (m *remoteAccessManager) applyIKEv2(inbound remoteAccessRuntimeInbound, callback *vpnSessionCallback) error {
	if runtime.GOOS != "linux" {
		return nil
	}
	if err := ensureIKEv2Prerequisites(); err != nil {
		return err
	}
	dir := filepath.Join(m.baseDir, "ikev2")
	if err := writeRemoteAccessFiles(dir, inbound, callback); err != nil {
		return err
	}
	poolConfigChanged, err := configureIKEv2Pool(dir, inbound.Users)
	if err != nil {
		return err
	}
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")
	caPath := filepath.Join(dir, "ca.crt")
	if err := writePEMSetting(certPath, inbound.Settings, "server_certificate"); err != nil {
		return err
	}
	if err := writePEMSetting(keyPath, inbound.Settings, "server_key"); err != nil {
		return err
	}
	if err := writePEMSetting(caPath, inbound.Settings, "ca_certificate"); err != nil {
		return err
	}
	if err := updateManagedBlock("/etc/ipsec.conf", "# BEGIN REBECCA IKEV2", "# END REBECCA IKEV2", ikev2IPSecConfig(inbound, certPath, caPath)); err != nil {
		return err
	}
	if err := updateManagedBlock("/etc/ipsec.secrets", "# BEGIN REBECCA IKEV2", "# END REBECCA IKEV2", ikev2Secrets(inbound, keyPath)); err != nil {
		return err
	}
	if err := applyRemoteAccessNetworking("ikev2", "", inbound); err != nil {
		return err
	}
	if poolConfigChanged {
		if output, restartErr := exec.Command("ipsec", "restart").CombinedOutput(); restartErr != nil {
			return fmt.Errorf("restart IKEv2 after enabling static address pools: %v: %s", restartErr, strings.TrimSpace(string(output)))
		}
		return nil
	}
	if output, err := exec.Command("ipsec", "rereadsecrets").CombinedOutput(); err != nil {
		return fmt.Errorf("reload IKEv2 secrets: %v: %s", err, strings.TrimSpace(string(output)))
	}
	if output, err := exec.Command("ipsec", "reload").CombinedOutput(); err != nil {
		return fmt.Errorf("reload IKEv2 config: %v: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (m *remoteAccessManager) applyAnyConnect(inbounds []remoteAccessRuntimeInbound, callback *vpnSessionCallback) error {
	if runtime.GOOS != "linux" {
		return nil
	}
	if err := ensureAnyConnectPrerequisites(); err != nil {
		return err
	}
	for i := range inbounds {
		left, err := netip.ParsePrefix(firstString(inbounds[i].Settings["ipv4_pool_cidr"]))
		if err != nil || !left.Addr().Is4() {
			return fmt.Errorf("AnyConnect inbound %s has an invalid IPv4 pool", inbounds[i].Tag)
		}
		for j := 0; j < i; j++ {
			right, _ := netip.ParsePrefix(firstString(inbounds[j].Settings["ipv4_pool_cidr"]))
			if left.Contains(right.Addr()) || right.Contains(left.Addr()) {
				return fmt.Errorf("AnyConnect inbounds %s and %s have overlapping IPv4 pools", inbounds[j].Tag, inbounds[i].Tag)
			}
		}
	}
	base := filepath.Join(m.baseDir, "anyconnect")
	desired := map[string]struct{}{}
	for _, inbound := range inbounds {
		name := safeName(inbound.Tag)
		desired[name] = struct{}{}
		dir := filepath.Join(base, name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		if err := writeRemoteAccessFiles(dir, inbound, callback); err != nil {
			return err
		}
		for path, key := range map[string]string{filepath.Join(dir, "server.crt"): "server_certificate", filepath.Join(dir, "server.key"): "server_key"} {
			if err := writePEMSetting(path, inbound.Settings, key); err != nil {
				return err
			}
		}
		if ca := firstString(inbound.Settings["ca_certificate"]); ca != "" {
			if err := writeFileIfChanged(filepath.Join(dir, "ca.crt"), []byte(ca+"\n"), 0o600); err != nil {
				return err
			}
		}
		if err := writeAnyConnectPAM(dir, name); err != nil {
			return err
		}
		if err := writeAnyConnectUserConfigs(dir, inbound.Users); err != nil {
			return err
		}
		conf := filepath.Join(dir, "ocserv.conf")
		oldConf, _ := os.ReadFile(conf)
		newConf := []byte(anyConnectConfig(inbound, dir, name))
		if err := writeFileIfChanged(conf, newConf, 0o600); err != nil {
			return err
		}
		if output, err := exec.Command("ocserv", "-t", "-c", conf).CombinedOutput(); err != nil {
			return fmt.Errorf("validate AnyConnect inbound %s: %v: %s", inbound.Tag, err, strings.TrimSpace(string(output)))
		}
		if err := applyRemoteAccessNetworking("anyconnect-"+name, anyConnectDevicePrefix(inbound.Port)+"*", inbound); err != nil {
			return err
		}
		if string(oldConf) != string(newConf) || !anyConnectRunning(dir) {
			if err := restartAnyConnect(name, conf, dir); err != nil {
				return err
			}
		}
		if err := terminateInvalidAnyConnectUsers(dir, inbound.Users); err != nil {
			return err
		}
	}
	if entries, err := os.ReadDir(base); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				if _, ok := desired[entry.Name()]; !ok {
					stopAnyConnect(entry.Name(), filepath.Join(base, entry.Name()))
				}
			}
		}
	}
	return nil
}

func writeRemoteAccessFiles(dir string, inbound remoteAccessRuntimeInbound, callback *vpnSessionCallback) error {
	if err := writeFileIfChanged(filepath.Join(dir, "users.tsv"), []byte(remoteAccessUsersTSV(inbound.Users)), 0o600); err != nil {
		return err
	}
	return writeVPNSessionCallback(vpnSessionCallbackPath(dir), callback)
}

func writePEMSetting(path string, settings map[string]any, key string) error {
	value := firstString(settings[key])
	if value == "" {
		return fmt.Errorf("%s is required", key)
	}
	return writeFileIfChanged(path, []byte(value+"\n"), 0o600)
}

func remoteAccessUsersTSV(users []remoteAccessRuntimeUser) string {
	var b strings.Builder
	for _, user := range users {
		limit, expire := "", ""
		if user.DataLimit != nil {
			limit = strconv.FormatInt(*user.DataLimit, 10)
		}
		if user.Expire != nil {
			expire = strconv.FormatInt(*user.Expire, 10)
		}
		line(&b, strings.Join([]string{strconv.FormatInt(user.UserID, 10), user.Username, user.Password, user.IPv4Address, strconv.FormatInt(user.UsedTraffic, 10), limit, user.Status, expire, strconv.FormatInt(user.DeviceLimit, 10)}, "\t"))
	}
	return b.String()
}

func ikev2IPSecConfig(inbound remoteAccessRuntimeInbound, certPath, caPath string) string {
	s := inbound.Settings
	auth := firstString(s["auth_mode"], "password")
	rightAuth, rightAuth2 := "eap-mschapv2", ""
	if auth == "certificate" {
		rightAuth = "pubkey"
	} else if auth == "password+certificate" {
		rightAuth, rightAuth2 = "pubkey", "eap-mschapv2"
	}
	var b strings.Builder
	line(&b, "conn rebecca-ikev2")
	line(&b, "  auto=add")
	line(&b, "  keyexchange=ikev2")
	line(&b, "  type=tunnel")
	line(&b, "  left=%any")
	line(&b, "  leftid="+firstString(s["server_identity"]))
	line(&b, "  leftcert="+certPath)
	line(&b, "  leftca="+caPath)
	leftSubnets := stringList(s["routes"])
	if boolValue(s["redirect_gateway"], true) || len(leftSubnets) == 0 {
		leftSubnets = []string{"0.0.0.0/0"}
	}
	line(&b, "  leftsubnet="+strings.Join(leftSubnets, ","))
	line(&b, "  leftsendcert="+map[bool]string{true: "always", false: "never"}[boolValue(s["send_cert"], true)])
	line(&b, "  right=%any")
	line(&b, "  rightauth="+rightAuth)
	if rightAuth2 != "" {
		line(&b, "  rightauth2="+rightAuth2)
	}
	line(&b, "  rightsourceip=%rebecca-ikev2")
	if dns := stringList(s["dns_servers"]); len(dns) > 0 {
		line(&b, "  rightdns="+strings.Join(dns, ","))
	}
	line(&b, "  eap_identity=%identity")
	line(&b, "  fragmentation="+firstString(s["fragmentation"], "yes"))
	line(&b, fmt.Sprintf("  mobike=%s", yesNo(boolValue(s["mobike"], true))))
	line(&b, fmt.Sprintf("  reauth=%s", yesNo(boolValue(s["reauth"], false))))
	line(&b, "  dpdaction=clear")
	line(&b, fmt.Sprintf("  dpddelay=%ds", boundedInt(s["dpd_delay"], 30, 0, 86400)))
	line(&b, fmt.Sprintf("  ikelifetime=%ds", boundedInt(s["ike_lifetime"], 10800, 60, 2592000)))
	line(&b, fmt.Sprintf("  lifetime=%ds", boundedInt(s["child_lifetime"], 3600, 60, 2592000)))
	line(&b, fmt.Sprintf("  rekeytime=%ds", boundedInt(s["rekey_time"], 3000, 0, 2592000)))
	line(&b, "  ike="+firstString(s["ike_proposals"]))
	line(&b, "  esp="+firstString(s["esp_proposals"]))
	return b.String()
}

func ikev2Secrets(inbound remoteAccessRuntimeInbound, keyPath string) string {
	var b strings.Builder
	line(&b, ": RSA "+keyPath)
	if firstString(inbound.Settings["auth_mode"], "password") != "certificate" {
		now := time.Now().Unix()
		for _, user := range inbound.Users {
			if user.Username != "" && user.Password != "" && remoteAccessRuntimeUserAvailable(user, now) {
				line(&b, fmt.Sprintf("%q : EAP %q", user.Username, user.Password))
			}
		}
	}
	return b.String()
}

func anyConnectConfig(inbound remoteAccessRuntimeInbound, dir, name string) string {
	s := inbound.Settings
	auth := firstString(s["auth_mode"], "password")
	var b strings.Builder
	if auth != "certificate" {
		line(&b, `auth = "pam"`)
	}
	if auth != "password" {
		line(&b, `auth = "certificate"`)
		line(&b, "ca-cert = "+filepath.Join(dir, "ca.crt"))
		line(&b, "cert-user-oid = "+firstString(s["cert_user_oid"], "2.5.4.3"))
	}
	if value := firstString(s["listen_host"]); value != "" {
		line(&b, "listen-host = "+value)
	}
	if value := firstString(s["udp_listen_host"]); value != "" && boolValue(s["udp_enabled"], true) {
		line(&b, "udp-listen-host = "+value)
	}
	if boolValue(s["listen_host_is_dyndns"], false) {
		line(&b, "listen-host-is-dyndns = true")
	}
	line(&b, "tcp-port = "+strconv.Itoa(inbound.Port))
	if boolValue(s["udp_enabled"], true) {
		line(&b, "udp-port = "+strconv.Itoa(boundedInt(s["udp_port"], inbound.Port, 1, 65535)))
	} else {
		line(&b, "udp-port = 0")
	}
	line(&b, "server-cert = "+filepath.Join(dir, "server.crt"))
	line(&b, "server-key = "+filepath.Join(dir, "server.key"))
	line(&b, "socket-file = "+filepath.Join(dir, "ocserv.sock"))
	line(&b, "pid-file = "+filepath.Join(dir, "ocserv.pid"))
	line(&b, "config-per-user = "+filepath.Join(dir, "users.d"))
	line(&b, "connect-script = "+filepath.Join(dir, "connect.sh"))
	line(&b, "device = "+anyConnectDevicePrefix(inbound.Port))
	line(&b, "ipv4-network = "+firstString(s["ipv4_pool_cidr"], "10.71.0.0/16"))
	for _, dns := range stringList(s["dns_servers"]) {
		line(&b, "dns = "+dns)
	}
	for _, nbns := range stringList(s["nbns_servers"]) {
		line(&b, "nbns = "+nbns)
	}
	for _, domain := range stringList(s["split_dns"]) {
		line(&b, "split-dns = "+domain)
	}
	for _, route := range stringList(s["routes"]) {
		line(&b, "route = "+route)
	}
	for _, route := range stringList(s["no_routes"]) {
		line(&b, "no-route = "+route)
	}
	if boolValue(s["redirect_gateway"], true) && len(stringList(s["routes"])) == 0 {
		line(&b, "route = default")
	}
	for _, item := range []struct {
		key, directive     string
		fallback, min, max int
	}{
		{"max_clients", "max-clients", 1024, 1, 1000000},
		{"max_same_clients", "max-same-clients", 0, 0, 1000000},
		{"cookie_timeout", "cookie-timeout", 300, 0, 2592000},
		{"auth_timeout", "auth-timeout", 240, 1, 86400},
		{"min_reauth_time", "min-reauth-time", 300, 0, 2592000},
		{"idle_timeout", "idle-timeout", 1200, 0, 2592000},
		{"mobile_idle_timeout", "mobile-idle-timeout", 2400, 0, 2592000},
		{"session_timeout", "session-timeout", 0, 0, 2592000},
		{"keepalive", "keepalive", 300, 0, 2592000},
		{"dpd", "dpd", 60, 0, 2592000},
		{"mobile_dpd", "mobile-dpd", 300, 0, 2592000},
		{"max_ban_score", "max-ban-score", 80, 0, 1000000},
		{"ban_reset_time", "ban-reset-time", 1200, 0, 2592000},
		{"rekey_time", "rekey-time", 172800, 0, 2592000},
		{"switch_to_tcp_timeout", "switch-to-tcp-timeout", 25, 0, 86400},
		{"rate_limit_ms", "rate-limit-ms", 100, 0, 60000},
		{"mtu", "mtu", 1400, 576, 1500},
		{"no_compress_limit", "no-compress-limit", 256, 0, 65535},
	} {
		line(&b, fmt.Sprintf("%s = %d", item.directive, anyConnectInt(s, item.key, item.fallback, item.min, item.max)))
	}
	for _, item := range []struct {
		key, directive string
		fallback       bool
	}{
		{"compression", "compression", false},
		{"cisco_client_compat", "cisco-client-compat", true},
		{"deny_roaming", "deny-roaming", false},
		{"tunnel_all_dns", "tunnel-all-dns", true},
		{"restrict_user_to_routes", "restrict-user-to-routes", false},
	} {
		line(&b, fmt.Sprintf("%s = %s", item.directive, yesNo(boolValue(s[item.key], item.fallback))))
	}
	for _, item := range [][2]string{
		{"persistent_cookies", "persistent-cookies"},
		{"try_mtu_discovery", "try-mtu-discovery"},
		{"ping_leases", "ping-leases"},
		{"cisco_svc_client_compat", "cisco-svc-client-compat"},
		{"client_bypass_protocol", "client-bypass-protocol"},
		{"match_tls_dtls_ciphers", "match-tls-dtls-ciphers"},
	} {
		if boolValue(s[item[0]], false) {
			line(&b, item[1]+" = true")
		}
	}
	for _, item := range [][2]string{{"dtls_psk", "dtls-psk"}, {"dtls_legacy", "dtls-legacy"}} {
		if !boolValue(s[item[0]], true) {
			line(&b, item[1]+" = false")
		}
	}
	for _, item := range [][2]string{
		{"stats_report_time", "stats-report-time"}, {"rx_data_per_sec", "rx-data-per-sec"},
		{"tx_data_per_sec", "tx-data-per-sec"}, {"output_buffer", "output-buffer"},
		{"net_priority", "net-priority"},
	} {
		if value := intValue(s[item[0]]); value > 0 {
			line(&b, fmt.Sprintf("%s = %d", item[1], value))
		}
	}
	if value := firstString(s["banner"]); value != "" {
		line(&b, "banner = "+strconv.Quote(value))
	}
	if value := firstString(s["pre_login_banner"]); value != "" {
		line(&b, "pre-login-banner = "+strconv.Quote(value))
	}
	if value := firstString(s["default_domain"]); value != "" {
		line(&b, "default-domain = "+value)
	}
	if value := firstString(s["restrict_user_to_ports"]); value != "" {
		line(&b, "restrict-user-to-ports = "+strconv.Quote(value))
	}
	line(&b, "rekey-method = "+firstString(s["rekey_method"], "ssl"))
	line(&b, "tls-priorities = "+strconv.Quote(firstString(s["tls_priorities"], "NORMAL:%SERVER_PRECEDENCE:%COMPAT:-VERS-SSL3.0:-VERS-TLS1.0:-VERS-TLS1.1")))
	line(&b, "isolate-workers = "+yesNo(anyConnectWorkerIsolationSafe(commandOutput("ocserv", "--version"))))
	line(&b, "predictable-ips = true")
	line(&b, "use-occtl = true")
	return b.String()
}

func anyConnectInt(settings map[string]any, key string, fallback, min, max int) int {
	value, ok := settings[key]
	if !ok || firstString(value) == "" {
		return fallback
	}
	return boundedInt(value, fallback, min, max)
}

func anyConnectWorkerIsolationSafe(version string) bool {
	return !strings.HasPrefix(strings.TrimSpace(version), "ocserv 1.1.")
}

func anyConnectDevicePrefix(port int) string {
	return "rac" + strconv.Itoa(port)
}

func writeAnyConnectPAM(dir, name string) error {
	base := filepath.Dir(dir)
	script := filepath.Join(base, "auth.sh")
	usersPath := filepath.Join(dir, "users.tsv")
	if err := writeFileIfChanged(script, []byte(anyConnectAuthScript(base)), 0o700); err != nil {
		return err
	}
	if err := writeFileIfChanged(filepath.Join(dir, "connect.sh"), []byte(anyConnectConnectScript(usersPath)), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile("/etc/pam.d/ocserv", []byte("auth required pam_exec.so expose_authtok quiet "+script+"\naccount required pam_permit.so\n"), 0o600); err != nil {
		return err
	}
	_ = os.Remove("/etc/pam.d/rebecca-ocserv-" + name)
	return nil
}

func anyConnectAuthScript(base string) string {
	return fmt.Sprintf(`#!/bin/sh
IFS= read -r password
now=$(date +%%s)
for users in %q/*/users.tsv; do
  [ -f "$users" ] || continue
  if awk -F '\t' -v u="$PAM_USER" -v p="$password" -v now="$now" '
    $2 == u && $3 == p && ($7 == "" || $7 == "active" || $7 == "on_hold") {
      if ($6 != "" && $5 >= $6) exit 2
      if ($8 != "" && now >= $8) exit 3
      found=1
      exit 0
    }
    END { exit found ? 0 : 1 }
  ' "$users"; then
    exit 0
  fi
done
exit 1
`, base)
}

func anyConnectConnectScript(usersPath string) string {
	return fmt.Sprintf(`#!/bin/sh
now=$(date +%%s)
awk -F '\t' -v u="$USERNAME" -v now="$now" '
  $2 == u && ($7 == "" || $7 == "active" || $7 == "on_hold") {
    if ($6 != "" && $5 >= $6) exit 2
    if ($8 != "" && now >= $8) exit 3
    found=1
    exit 0
  }
  END { exit found ? 0 : 1 }
' %q
`, usersPath)
}

func writeAnyConnectUserConfigs(dir string, users []remoteAccessRuntimeUser) error {
	usersDir := filepath.Join(dir, "users.d")
	if err := os.MkdirAll(usersDir, 0o700); err != nil {
		return err
	}
	desired := map[string]struct{}{}
	for _, user := range users {
		name := safeName(user.Username)
		desired[name] = struct{}{}
		if err := writeFileIfChanged(filepath.Join(usersDir, name), []byte("explicit-ipv4 = "+user.IPv4Address+"\n"), 0o600); err != nil {
			return err
		}
	}
	if entries, err := os.ReadDir(usersDir); err == nil {
		for _, entry := range entries {
			if _, ok := desired[entry.Name()]; !ok {
				_ = os.Remove(filepath.Join(usersDir, entry.Name()))
			}
		}
	}
	return nil
}

func applyRemoteAccessNetworking(name, iface string, inbound remoteAccessRuntimeInbound) error {
	pool := firstString(inbound.Settings["ipv4_pool_cidr"])
	if inbound.TunnelPort > 0 && boolValue(inbound.Settings["tproxy_enabled"], true) {
		enableVPNTProxyHostNetworking(pool)
		table := "rebecca_" + safeName(name)
		match := "ip saddr " + pool
		if iface != "" {
			match = `iifname "` + iface + `"`
		}
		rules := remoteAccessTProxyScript(table, match, pool, inbound.TunnelPort)
		_ = exec.Command("nft", "delete", "table", "inet", table).Run()
		cmd := exec.Command("nft", "-f", "-")
		cmd.Stdin = strings.NewReader(rules)
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("apply %s nftables: %v: %s", name, err, strings.TrimSpace(string(output)))
		}
		if err := applyTProxyRouting(); err != nil {
			return err
		}
		return vpnRemoveDirectNAT(name)
	}
	return vpnApplyDirectNAT(name, iface, pool)
}

func remoteAccessTProxyScript(table, match, pool string, tunnelPort int) string {
	return fmt.Sprintf(`table inet %s {
  chain prerouting {
    type filter hook prerouting priority mangle; policy accept;
    %s meta mark != 0xff meta l4proto { tcp, udp } tproxy ip to 127.0.0.1:%d meta mark set 1 accept
  }
  chain postrouting {
    type nat hook postrouting priority srcnat; policy accept;
    ip saddr %s meta l4proto icmp masquerade
  }
}
`, table, match, tunnelPort, pool)
}

func (m *remoteAccessManager) stop(protocol string) error {
	if runtime.GOOS != "linux" {
		return nil
	}
	if protocol == "ikev2" {
		_ = updateManagedBlock("/etc/ipsec.conf", "# BEGIN REBECCA IKEV2", "# END REBECCA IKEV2", "")
		_ = updateManagedBlock("/etc/ipsec.secrets", "# BEGIN REBECCA IKEV2", "# END REBECCA IKEV2", "")
		_ = runOptional("ipsec", "reload")
		return nil
	}
	base := filepath.Join(m.baseDir, "anyconnect")
	entries, _ := os.ReadDir(base)
	for _, entry := range entries {
		if entry.IsDir() {
			stopAnyConnect(entry.Name(), filepath.Join(base, entry.Name()))
		}
	}
	return nil
}

func ensureIKEv2Prerequisites() error {
	if runtime.GOOS != "linux" {
		return nil
	}
	if strings.Contains(strings.ToLower(commandOutput("ipsec", "--version")), "strongswan") && commandExists("swanctl") && commandExists("sqlite3") {
		return nil
	}
	if os.Geteuid() != 0 || !commandExists("apt-get") {
		return fmt.Errorf("IKEv2 requires strongSwan and automatic migration is supported on apt-based binary nodes")
	}
	_ = runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "remove", "-y", "libreswan")
	if err := runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "update"); err != nil {
		return err
	}
	return runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "install", "-y", "--no-install-recommends", "strongswan", "strongswan-starter", "strongswan-swanctl", "libcharon-extra-plugins", "libstrongswan-extra-plugins", "sqlite3", "nftables", "iptables")
}

func configureIKEv2Pool(dir string, users []remoteAccessRuntimeUser) (bool, error) {
	database := filepath.Join(dir, "leases.db")
	config := fmt.Sprintf("charon {\n  plugins {\n    attr-sql {\n      database = sqlite://%s\n      lease_history = yes\n    }\n  }\n}\npool {\n  database = sqlite://%s\n  load = sqlite\n}\n", database, database)
	before, _ := os.ReadFile("/etc/strongswan.conf")
	if err := updateManagedBlock("/etc/strongswan.conf", "# BEGIN REBECCA IKEV2 POOL", "# END REBECCA IKEV2 POOL", config); err != nil {
		return false, err
	}
	after, _ := os.ReadFile("/etc/strongswan.conf")
	configChanged := string(before) != string(after)
	databaseCreated := false
	if _, err := os.Stat(database); os.IsNotExist(err) {
		databaseCreated = true
		schema := ""
		for _, candidate := range []string{"/usr/share/strongswan/templates/database/sqlite.sql", "/usr/share/strongswan/templates/database/sqlite.sql.gz", "/usr/share/doc/strongswan/examples/sqlite.sql"} {
			if _, statErr := os.Stat(candidate); statErr == nil {
				schema = candidate
				break
			}
		}
		if schema == "" || strings.HasSuffix(schema, ".gz") {
			return false, fmt.Errorf("strongSwan SQLite pool schema was not installed")
		}
		cmd := exec.Command("sqlite3", database)
		raw, readErr := os.ReadFile(schema)
		if readErr != nil {
			return false, readErr
		}
		cmd.Stdin = strings.NewReader(string(raw))
		if output, runErr := cmd.CombinedOutput(); runErr != nil {
			return false, fmt.Errorf("initialize IKEv2 address pool: %v: %s", runErr, strings.TrimSpace(string(output)))
		}
		_ = os.Chmod(database, 0o600)
	}
	var leases strings.Builder
	for _, user := range users {
		if user.Username != "" && user.IPv4Address != "" && !strings.ContainsAny(user.Username, "\r\n=") {
			line(&leases, user.IPv4Address+"="+user.Username)
		}
	}
	addresses := filepath.Join(dir, "addresses.txt")
	old, _ := os.ReadFile(addresses)
	if !databaseCreated && string(old) == leases.String() {
		return configChanged, nil
	}
	if err := writeFileIfChanged(addresses, []byte(leases.String()), 0o600); err != nil {
		return false, err
	}
	args := []string{"pool", "--replace", "rebecca-ikev2", "--addresses", addresses, "--timeout", "0"}
	if output, err := exec.Command("ipsec", args...).CombinedOutput(); err != nil {
		args[1] = "--add"
		if retry, retryErr := exec.Command("ipsec", args...).CombinedOutput(); retryErr != nil {
			return false, fmt.Errorf("configure IKEv2 address pool: %v: %s; add: %v: %s", err, strings.TrimSpace(string(output)), retryErr, strings.TrimSpace(string(retry)))
		}
	}
	return configChanged, nil
}

func ensureAnyConnectPrerequisites() error {
	if runtime.GOOS != "linux" {
		return nil
	}
	if commandExists("ocserv") {
		_ = exec.Command("systemctl", "disable", "--now", "ocserv.service").Run()
		return nil
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("AnyConnect prerequisites are missing and automatic install requires root")
	}
	var err error
	switch {
	case commandExists("apt-get"):
		if err = runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "update"); err == nil {
			err = runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "install", "-y", "--no-install-recommends", "ocserv", "libpam-modules", "nftables", "iptables")
		}
	case commandExists("dnf"):
		err = runInstallCommand(nil, "dnf", "install", "-y", "ocserv", "nftables", "iptables")
	case commandExists("yum"):
		err = runInstallCommand(nil, "yum", "install", "-y", "ocserv", "nftables", "iptables")
	default:
		return fmt.Errorf("AnyConnect prerequisites are missing and no supported package manager was found")
	}
	if err == nil {
		_ = exec.Command("systemctl", "disable", "--now", "ocserv.service").Run()
	}
	return err
}

func restartAnyConnect(name, conf, dir string) error {
	stopAnyConnect(name, dir)
	cmd := exec.Command("ocserv", "--foreground", "-c", conf)
	logPath := filepath.Join(dir, "ocserv.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	_ = logFile.Close()
	go func() { _ = cmd.Wait() }()
	for range 20 {
		if anyConnectRunning(dir) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("AnyConnect inbound %s exited before becoming ready; see %s", name, logPath)
}

func anyConnectRunning(dir string) bool {
	raw, err := os.ReadFile(filepath.Join(dir, "ocserv.pid"))
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	return err == nil && pid > 1 && processAlive(pid)
}

func processAlive(pid int) bool {
	if raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat")); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) > 2 && fields[2] == "Z" {
			return false
		}
	}
	return exec.Command("kill", "-0", strconv.Itoa(pid)).Run() == nil
}

func terminateInvalidAnyConnectUsers(dir string, users []remoteAccessRuntimeUser) error {
	allowed := make(map[string]struct{}, len(users))
	now := time.Now().Unix()
	for _, user := range users {
		if remoteAccessRuntimeUserAvailable(user, now) {
			allowed[user.Username] = struct{}{}
		}
	}
	output, err := exec.Command("occtl", "--json", "--socket-file", filepath.Join(dir, "ocserv.sock"), "show", "users").Output()
	if err != nil {
		return nil
	}
	var sessions []map[string]any
	if json.Unmarshal(output, &sessions) != nil {
		return nil
	}
	for _, session := range sessions {
		username := firstString(session["Username"], session["username"])
		if username == "" {
			continue
		}
		if _, ok := allowed[username]; ok {
			continue
		}
		_ = exec.Command("occtl", "--socket-file", filepath.Join(dir, "ocserv.sock"), "terminate", "user", username).Run()
	}
	return nil
}

func disconnectAnyConnectSession(dir string, session map[string]any) {
	socketArgs := []string{"--socket-file", filepath.Join(dir, "ocserv.sock")}
	if id := firstString(session["ID"]); id != "" {
		if exec.Command("occtl", append(socketArgs, "disconnect", "id", id)...).Run() == nil {
			return
		}
	}
	if username := firstString(session["Username"], session["username"]); username != "" {
		_ = exec.Command("occtl", append(socketArgs, "terminate", "user", username)...).Run()
	}
}

func stopAnyConnect(name, dir string) {
	pidPath := filepath.Join(dir, "ocserv.pid")
	if raw, err := os.ReadFile(pidPath); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil && pid > 1 {
			_ = exec.Command("kill", strconv.Itoa(pid)).Run()
			for range 40 {
				if !processAlive(pid) {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if processAlive(pid) {
				_ = exec.Command("kill", "-KILL", strconv.Itoa(pid)).Run()
			}
		}
	}
	_ = os.Remove(pidPath)
	_ = exec.Command("nft", "delete", "table", "inet", "rebecca_anyconnect_"+safeName(name)).Run()
	_ = vpnRemoveDirectNAT("anyconnect-" + name)
}

func yesNo(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func (m *remoteAccessManager) CollectUsage(protocol string) []xray.UserStat {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	totals := map[userUsageKey]int64{}
	base := filepath.Join(m.baseDir, protocol)
	entries := []string{base}
	if protocol == "anyconnect" {
		entries = nil
		if dirs, err := os.ReadDir(base); err == nil {
			for _, dir := range dirs {
				if dir.IsDir() {
					entries = append(entries, filepath.Join(base, dir.Name()))
				}
			}
		}
	}
	runtimeConfig := m.runtimeConfig(protocol)
	for _, dir := range entries {
		inboundTag := ""
		if runtimeConfig != nil {
			for _, inbound := range runtimeConfig.Inbounds {
				if protocol != "anyconnect" || safeName(inbound.Tag) == filepath.Base(dir) {
					inboundTag = inbound.Tag
					break
				}
			}
		}
		stats := map[string]int64{}
		if protocol == "anyconnect" {
			m.collectAnyConnectLive(dir, stats)
		} else if protocol == "ikev2" {
			m.collectIKEv2Live(dir, stats)
		}
		file, err := os.Open(filepath.Join(dir, "usage.tsv"))
		if err == nil {
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				parts := strings.Split(scanner.Text(), "\t")
				if len(parts) >= 2 {
					value, _ := strconv.ParseInt(parts[1], 10, 64)
					stats[protocol+":"+parts[0]] += value
				}
			}
			_ = file.Close()
			_ = os.WriteFile(filepath.Join(dir, "usage.tsv"), nil, 0o600)
		}
		for uid, value := range stats {
			addUserUsage(totals, uid, inboundTag, value)
		}
	}
	return userUsageStats(totals)
}

type ikev2Session struct {
	ID, Username, ClientIP, AssignedIP string
	Bytes                              int64
}

var ikev2SARx = regexp.MustCompile(`(?s)(?:^|[\s{])[^\s{}]+\s+\{uniqueid=([0-9]+).*?remote-id=([^\s}]+).*?remote-host=([^\s}]+).*?bytes-in=([0-9]+).*?bytes-out=([0-9]+).*?remote-ts=\[([0-9a-fA-F:.]+)/[0-9]+\]`)

func parseIKEv2SAs(raw string) []ikev2Session {
	matches := ikev2SARx.FindAllStringSubmatch(raw, -1)
	result := make([]ikev2Session, 0, len(matches))
	for _, match := range matches {
		rx, _ := strconv.ParseInt(match[4], 10, 64)
		tx, _ := strconv.ParseInt(match[5], 10, 64)
		result = append(result, ikev2Session{ID: match[1], Username: strings.Trim(match[2], "'\""), ClientIP: match[3], AssignedIP: match[6], Bytes: rx + tx})
	}
	return result
}

func (m *remoteAccessManager) collectIKEv2Live(dir string, stats map[string]int64) {
	users := readRemoteAccessUsers(filepath.Join(dir, "users.tsv"))
	if len(users) == 0 || !commandExists("swanctl") {
		return
	}
	output, err := exec.Command("swanctl", "--list-sas", "--ike", "rebecca-ikev2", "--raw", "--noblock").Output()
	if err != nil {
		return
	}
	accountingPath := filepath.Join(dir, "accounting.tsv")
	records := readOVAccounting(accountingPath)
	active := map[string]struct{}{}
	runtimeConfig := m.runtimeConfig("ikev2")
	callback, inboundTag, accounting := (*vpnSessionCallback)(nil), "ikev2", true
	if runtimeConfig != nil {
		callback = runtimeConfig.SessionCallback
		if len(runtimeConfig.Inbounds) > 0 {
			inboundTag = runtimeConfig.Inbounds[0].Tag
			accounting = boolValue(runtimeConfig.Inbounds[0].Settings["accounting_enabled"], true)
		}
	}
	now := time.Now().Unix()
	for _, session := range parseIKEv2SAs(string(output)) {
		user, ok := users[session.Username]
		if !ok {
			_ = exec.Command("swanctl", "--terminate", "--ike-id", session.ID).Run()
			continue
		}
		record, existed := records[session.ID]
		if record.UserID == "" {
			record.UserID, record.Base = strconv.FormatInt(user.UserID, 10), user.Used
		}
		if accounting && session.Bytes > record.Total {
			stats["ikev2:"+record.UserID] += session.Bytes - record.Total
		}
		record.Total = session.Bytes
		records[session.ID], active[session.ID] = record, struct{}{}
		if !remoteAccessSnapshotAvailable(user, now) {
			_ = exec.Command("swanctl", "--terminate", "--ike-id", session.ID).Run()
			continue
		}
		if !existed {
			event := vpnSessionEvent{UserID: user.UserID, Protocol: "ikev2", InboundTag: inboundTag, SessionID: session.ID, AssignedIP: session.AssignedIP, ClientIP: session.ClientIP, Event: "start"}
			if !vpnAdmitGoSession(m.sessionsPath(), callback, event, user.DeviceLimit) {
				_ = exec.Command("swanctl", "--terminate", "--ike-id", session.ID).Run()
				continue
			}
		}
		if user.Limit > 0 && record.Base+record.Total >= user.Limit {
			_ = exec.Command("swanctl", "--terminate", "--ike-id", session.ID).Run()
		}
	}
	for sessionID, record := range records {
		if _, ok := active[sessionID]; ok {
			continue
		}
		uid, _ := strconv.ParseInt(record.UserID, 10, 64)
		vpnReleaseGoSession(m.sessionsPath(), callback, vpnSessionEvent{UserID: uid, Protocol: "ikev2", InboundTag: inboundTag, SessionID: sessionID, Event: "stop"})
		delete(records, sessionID)
	}
	writeOVAccounting(accountingPath, records)
}

func (m *remoteAccessManager) runtimeConfig(protocol string) *remoteAccessRuntime {
	var runtimeConfig remoteAccessRuntime
	raw, err := os.ReadFile(filepath.Join(m.baseDir, protocol, "runtime.json"))
	if err != nil || json.Unmarshal(raw, &runtimeConfig) != nil {
		return nil
	}
	return &runtimeConfig
}

type remoteAccessUserSnapshot struct {
	UserID      int64
	Used        int64
	Limit       int64
	DeviceLimit int64
	AssignedIP  string
	Status      string
	Expire      int64
}

func readRemoteAccessUsers(path string) map[string]remoteAccessUserSnapshot {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	result := map[string]remoteAccessUserSnapshot{}
	for _, row := range strings.Split(string(raw), "\n") {
		parts := strings.Split(row, "\t")
		if len(parts) < 9 || parts[0] == "" || parts[1] == "" {
			continue
		}
		uid, _ := strconv.ParseInt(parts[0], 10, 64)
		used, _ := strconv.ParseInt(parts[4], 10, 64)
		limit, _ := strconv.ParseInt(parts[5], 10, 64)
		expire, _ := strconv.ParseInt(parts[7], 10, 64)
		deviceLimit, _ := strconv.ParseInt(parts[8], 10, 64)
		result[parts[1]] = remoteAccessUserSnapshot{UserID: uid, Used: used, Limit: limit, DeviceLimit: deviceLimit, AssignedIP: parts[3], Status: parts[6], Expire: expire}
	}
	return result
}

func remoteAccessRuntimeUserAvailable(user remoteAccessRuntimeUser, now int64) bool {
	return runtimeUserAvailable(user.Status, user.UsedTraffic, user.DataLimit, user.Expire, now)
}

func runtimeUserAvailable(status string, used int64, dataLimit, expiresAt *int64, now int64) bool {
	limit, expire := int64(0), int64(0)
	if dataLimit != nil {
		limit = *dataLimit
	}
	if expiresAt != nil {
		expire = *expiresAt
	}
	return remoteAccessUserAvailable(status, used, limit, expire, now)
}

func remoteAccessSnapshotAvailable(user remoteAccessUserSnapshot, now int64) bool {
	return remoteAccessUserAvailable(user.Status, user.Used, user.Limit, user.Expire, now)
}

func remoteAccessUserAvailable(status string, used, limit, expire, now int64) bool {
	status = strings.ToLower(strings.TrimSpace(status))
	if status != "" && status != "active" && status != "on_hold" {
		return false
	}
	if limit > 0 && used >= limit {
		return false
	}
	return expire <= 0 || now < expire
}

func (m *remoteAccessManager) collectAnyConnectLive(dir string, stats map[string]int64) {
	users := readRemoteAccessUsers(filepath.Join(dir, "users.tsv"))
	if len(users) == 0 {
		return
	}
	output, err := exec.Command("occtl", "--json", "--socket-file", filepath.Join(dir, "ocserv.sock"), "show", "users").Output()
	if err != nil {
		return
	}
	var sessions []map[string]any
	if json.Unmarshal(output, &sessions) != nil {
		return
	}
	links := interfaceByteTotals()
	accountingPath := filepath.Join(dir, "accounting.tsv")
	records := readOVAccounting(accountingPath)
	active := map[string]struct{}{}
	runtimeConfig := m.runtimeConfig("anyconnect")
	callback, accounting := (*vpnSessionCallback)(nil), true
	if runtimeConfig != nil {
		callback = runtimeConfig.SessionCallback
		for _, inbound := range runtimeConfig.Inbounds {
			if safeName(inbound.Tag) == filepath.Base(dir) {
				accounting = boolValue(inbound.Settings["accounting_enabled"], true)
				break
			}
		}
	}
	now := time.Now().Unix()
	for _, session := range sessions {
		username := firstString(session["Username"], session["username"])
		user, ok := users[username]
		if !ok {
			continue
		}
		sessionID := firstString(session["Full session"], session["Session"], session["ID"])
		if sessionID == "" {
			continue
		}
		device := firstString(session["Device"], session["device"])
		total := links[device]
		if total == 0 {
			total = parseHumanBytes(firstString(session["RX"])) + parseHumanBytes(firstString(session["TX"]))
		}
		record, existed := records[sessionID]
		if record.UserID == "" {
			record.UserID = strconv.FormatInt(user.UserID, 10)
			record.Base = user.Used
		}
		if accounting && total > record.Total {
			stats["anyconnect:"+record.UserID] += total - record.Total
		}
		record.Total = total
		records[sessionID], active[sessionID] = record, struct{}{}
		assignedIP := firstString(session["IPv4"], session["IP"], user.AssignedIP)
		clientIP := firstString(session["Remote IP"])
		if !remoteAccessSnapshotAvailable(user, now) {
			disconnectAnyConnectSession(dir, session)
			continue
		}
		if !existed {
			event := vpnSessionEvent{UserID: user.UserID, Protocol: "anyconnect", InboundTag: filepath.Base(dir), SessionID: sessionID, AssignedIP: assignedIP, ClientIP: clientIP, Event: "start"}
			if !vpnAdmitGoSession(m.sessionsPath(), callback, event, user.DeviceLimit) {
				disconnectAnyConnectSession(dir, session)
				continue
			}
		}
		if user.Limit > 0 && record.Base+record.Total >= user.Limit {
			_ = exec.Command("occtl", "--socket-file", filepath.Join(dir, "ocserv.sock"), "terminate", "user", username).Run()
		}
	}
	for sessionID, record := range records {
		if _, ok := active[sessionID]; ok {
			continue
		}
		uid, _ := strconv.ParseInt(record.UserID, 10, 64)
		vpnReleaseGoSession(m.sessionsPath(), callback, vpnSessionEvent{UserID: uid, Protocol: "anyconnect", InboundTag: filepath.Base(dir), SessionID: sessionID, Event: "stop"})
		delete(records, sessionID)
	}
	writeOVAccounting(accountingPath, records)
}

func interfaceByteTotals() map[string]int64 {
	output, err := exec.Command("ip", "-s", "-j", "link", "show").Output()
	if err != nil {
		return nil
	}
	var links []struct {
		Name  string `json:"ifname"`
		Stats struct {
			RX struct {
				Bytes int64 `json:"bytes"`
			} `json:"rx"`
			TX struct {
				Bytes int64 `json:"bytes"`
			} `json:"tx"`
		} `json:"stats64"`
	}
	if json.Unmarshal(output, &links) != nil {
		return nil
	}
	result := make(map[string]int64, len(links))
	for _, link := range links {
		result[link.Name] = link.Stats.RX.Bytes + link.Stats.TX.Bytes
	}
	return result
}

func parseHumanBytes(value string) int64 {
	fields := strings.Fields(strings.TrimSpace(value))
	if len(fields) == 0 {
		return 0
	}
	number, _ := strconv.ParseFloat(fields[0], 64)
	multiplier := float64(1)
	if len(fields) > 1 {
		switch strings.ToUpper(strings.TrimSpace(fields[1])) {
		case "KB", "KIB":
			multiplier = 1024
		case "MB", "MIB":
			multiplier = 1024 * 1024
		case "GB", "GIB":
			multiplier = 1024 * 1024 * 1024
		case "TB", "TIB":
			multiplier = 1024 * 1024 * 1024 * 1024
		}
	}
	return int64(number * multiplier)
}
