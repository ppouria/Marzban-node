//go:build linux

package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	webInstallerURL = "https://raw.githubusercontent.com/iliya-Developer/tproxy-web/cf92ae7dfdf3024b3bfa19ebe8c28f89e84d105c/install.sh"
	webInstallerSHA = "33c0622cdddf8f959891af54e3f5e174558a047f388e5aceacaa2ec5e69df795"
)

type externalProxyManager struct {
	mu            sync.Mutex
	dir           string
	mtProcesses   map[string]*exec.Cmd
	webInstalling bool
	runtime       *extraRuntime
	updateChannel func() string
}

func newExternalProxyManager(dataDir string, updateChannel func() string) *externalProxyManager {
	return &externalProxyManager{dir: filepath.Join(dataDir, "external-proxies"), mtProcesses: map[string]*exec.Cmd{}, updateChannel: updateChannel}
}

func (m *externalProxyManager) Apply(runtimeConfig *extraRuntime) error {
	if runtimeConfig == nil {
		runtimeConfig = &extraRuntime{Inbounds: []extraRuntimeInbound{}}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mt, web := []extraRuntimeInbound{}, []extraRuntimeInbound{}
	for _, inbound := range runtimeConfig.Inbounds {
		switch strings.ToLower(strings.TrimSpace(inbound.Protocol)) {
		case "mtproto":
			mt = append(mt, inbound)
		case "web":
			web = append(web, inbound)
		}
	}
	if len(web) > 1 {
		return errors.New("only one WEB proxy inbound is allowed per node")
	}
	if err := m.validateWebLocked(web); err != nil {
		return err
	}
	if err := m.applyMTProxyLocked(mt); err != nil {
		return err
	}
	if err := m.applyWebLocked(web); err != nil {
		return err
	}
	m.runtime = runtimeConfig
	return nil
}

func (m *externalProxyManager) validateWebLocked(inbounds []extraRuntimeInbound) error {
	if len(inbounds) == 0 {
		return nil
	}
	if os.Geteuid() != 0 || runtime.GOARCH != "amd64" {
		return errors.New("WEB proxy requires a root-run x86_64 Linux node")
	}
	if err := validateWebPlatform(); err != nil {
		return err
	}
	statePath := filepath.Join(m.dir, "web.json")
	if fileExists("/etc/tproxy-server/manager.env") && !fileExists(statePath) {
		return errors.New("a WEB proxy installation not managed by Rebecca already exists on this node")
	}
	_, err := webDNSNeedsConfirmation(stringSetting(inbounds[0].Settings, "hostname"))
	return err
}

func (m *externalProxyManager) applyMTProxyLocked(inbounds []extraRuntimeInbound) error {
	if len(inbounds) == 0 {
		m.stopMTProxyLocked()
		return nil
	}
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		return errors.New("MTProxy requires a Linux amd64 or arm64 node")
	}
	binary, err := m.ensureTelemtLocked()
	if err != nil {
		return err
	}
	type preparedInbound struct {
		inbound     extraRuntimeInbound
		dir, config string
	}
	prepared := make([]preparedInbound, 0, len(inbounds))
	for _, inbound := range inbounds {
		dir := filepath.Join(m.dir, "mtproto", safeName(inbound.Tag))
		config, err := telemtConfig(inbound)
		if err != nil {
			return fmt.Errorf("MTProxy %q: %w", inbound.Tag, err)
		}
		prepared = append(prepared, preparedInbound{inbound: inbound, dir: dir, config: config})
	}
	for _, item := range prepared {
		if err := os.MkdirAll(item.dir, 0o700); err != nil {
			return err
		}
		configPath := filepath.Join(item.dir, "config.toml")
		if err := writeFileAtomic(configPath, []byte(item.config), 0o600); err != nil {
			return err
		}
	}
	m.stopMTProxyLocked()
	for _, item := range prepared {
		inbound, dir := item.inbound, item.dir
		configPath := filepath.Join(dir, "config.toml")
		logFile, err := os.OpenFile(filepath.Join(m.dir, safeName(inbound.Tag)+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		command := exec.Command(binary, "--data-path", dir, configPath)
		command.Dir = dir
		command.Stdout, command.Stderr = logFile, logFile
		if err := command.Start(); err != nil {
			_ = logFile.Close()
			return fmt.Errorf("start MTProxy %q: %w", inbound.Tag, err)
		}
		m.mtProcesses[inbound.Tag] = command
		go func() {
			_ = command.Wait()
			_ = logFile.Close()
		}()
	}
	return nil
}

func (m *externalProxyManager) stopMTProxyLocked() {
	for tag, process := range m.mtProcesses {
		if process.Process != nil {
			_ = process.Process.Kill()
		}
		delete(m.mtProcesses, tag)
	}
}

func (m *externalProxyManager) ensureTelemtLocked() (string, error) {
	if err := os.MkdirAll(m.dir, 0o700); err != nil {
		return "", err
	}
	binary := filepath.Join(m.dir, "telemt")
	if fileExists(binary) {
		return binary, nil
	}
	asset := "rebecca-telemt-linux-" + runtime.GOARCH
	base := "https://github.com/rebeccapanel/rebecca-node/releases/latest/download/"
	if m.updateChannel != nil && m.updateChannel() == "dev" {
		asset = "rebecca-telemt-dev-linux-" + runtime.GOARCH
		base = "https://github.com/rebeccapanel/rebecca-node/releases/download/dev-binaries/"
	}
	body, err := download(base+asset, 10*time.Minute)
	if err != nil {
		return "", fmt.Errorf("download patched telemt: %w", err)
	}
	checksum, err := download(base+asset+".sha256", 30*time.Second)
	if err != nil {
		return "", fmt.Errorf("download patched telemt checksum: %w", err)
	}
	want := strings.Fields(string(checksum))
	sum := sha256.Sum256(body)
	if len(want) == 0 || !strings.EqualFold(want[0], hex.EncodeToString(sum[:])) {
		return "", errors.New("patched telemt checksum mismatch")
	}
	if len(body) < 1<<20 || len(body) > 100<<20 {
		return "", errors.New("patched telemt binary has an unexpected size")
	}
	if err := writeFileAtomic(binary, body, 0o700); err != nil {
		return "", err
	}
	return binary, nil
}

func telemtConfig(inbound extraRuntimeInbound) (string, error) {
	secret := strings.ToLower(stringSetting(inbound.Settings, "secret"))
	if len(secret) != 32 {
		return "", errors.New("secret must be 32 hexadecimal characters")
	}
	if _, err := hex.DecodeString(secret); err != nil {
		return "", errors.New("secret must be 32 hexadecimal characters")
	}
	modes := []string{}
	for _, mode := range []struct{ key, name string }{{"mode_classic", "classic"}, {"mode_secure", "secure"}, {"mode_tls", "tls"}} {
		if boolSetting(inbound.Settings, mode.key) {
			modes = append(modes, mode.name)
		}
	}
	if len(modes) == 0 {
		return "", errors.New("at least one connection mode is required")
	}
	sponsor := strings.ToLower(stringSetting(inbound.Settings, "sponsor_tag"))
	if sponsor != "" {
		if len(sponsor) != 32 {
			return "", errors.New("sponsor tag must be 32 hexadecimal characters")
		}
		if _, err := hex.DecodeString(sponsor); err != nil {
			return "", errors.New("sponsor tag must be 32 hexadecimal characters")
		}
	}
	listen := strings.TrimSpace(inbound.Listen)
	if net.ParseIP(listen) == nil {
		return "", errors.New("listen address must be a numeric IP")
	}
	if inbound.Port < 1 || inbound.Port > 65535 {
		return "", errors.New("port must be between 1 and 65535")
	}
	userLimit := intSetting(inbound.Settings, "user_limit", 0)
	if userLimit < 0 || userLimit > 64 {
		return "", errors.New("unique IP limit must be between 0 and 64")
	}
	maxConnections := intSetting(inbound.Settings, "max_connections", 0)
	if maxConnections < 0 || maxConnections > 1000000 {
		return "", errors.New("maximum connections must be between 0 and 1000000")
	}
	tlsDomain := strings.ToLower(stringSetting(inbound.Settings, "tls_domain"))
	if tlsDomain == "" {
		tlsDomain = "www.google.com"
	}
	var config strings.Builder
	fmt.Fprintf(&config, "[general]\nuse_middle_proxy = %t\nlog_level = \"normal\"\ndisable_colors = true\n", sponsor != "")
	if sponsor != "" {
		fmt.Fprintf(&config, "ad_tag = %q\n", sponsor)
	}
	fmt.Fprintf(&config, "\n[general.modes]\nclassic = %t\nsecure = %t\ntls = %t\n", boolSetting(inbound.Settings, "mode_classic"), boolSetting(inbound.Settings, "mode_secure"), boolSetting(inbound.Settings, "mode_tls"))
	fmt.Fprintf(&config, "\n[server]\nport = %d\nlisten_addr_ipv4 = %q\n\n[server.api]\nenabled = false\n", inbound.Port, listen)
	fmt.Fprintf(&config, "\n[censorship]\ntls_domain = %q\nunknown_sni_action = \"accept\"\nmask = true\ntls_emulation = true\n", tlsDomain)
	fmt.Fprintf(&config, "\n[access]\nuser_max_tcp_conns_global_each = %d\n\n[access.users]\nrebecca = %q\n\n[access.user_enabled]\nrebecca = true\n", maxConnections, secret)
	fmt.Fprintf(&config, "\n[access.user_modes]\nrebecca = %q\n\n[access.user_max_unique_ips]\nrebecca = %d\n", strings.Join(modes, ","), userLimit)
	config.WriteString("\n[[upstreams]]\ntype = \"direct\"\n")
	return config.String(), nil
}

func (m *externalProxyManager) applyWebLocked(inbounds []extraRuntimeInbound) error {
	statePath := filepath.Join(m.dir, "web.json")
	if len(inbounds) == 0 {
		if fileExists(statePath) && !m.webInstalling {
			installerPath := filepath.Join(m.dir, "tproxy-web-install.sh")
			logFile, err := os.OpenFile(filepath.Join(m.dir, "web-install.log"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			command := exec.Command("bash", installerPath, "uninstall")
			command.Stdin = strings.NewReader("y\nn\n")
			command.Stdout, command.Stderr = logFile, logFile
			if err := command.Start(); err != nil {
				_ = logFile.Close()
				return err
			}
			m.webInstalling = true
			go func() {
				err := command.Wait()
				_ = logFile.Close()
				m.mu.Lock()
				defer m.mu.Unlock()
				m.webInstalling = false
				if err == nil {
					_ = os.Remove(statePath)
				}
			}()
		}
		return nil
	}
	desired, _ := json.Marshal(inbounds[0])
	if current, err := os.ReadFile(statePath); err == nil && bytes.Equal(current, desired) {
		return nil
	}
	if m.webInstalling {
		return nil
	}
	reinstall := fileExists("/etc/tproxy-server/manager.env") && fileExists(statePath)
	dnsConfirmation, err := webDNSNeedsConfirmation(stringSetting(inbounds[0].Settings, "hostname"))
	if err != nil {
		return err
	}
	installer, err := download(webInstallerURL, 2*time.Minute)
	if err != nil {
		return fmt.Errorf("download WEB proxy installer: %w", err)
	}
	if sum := sha256.Sum256(installer); hex.EncodeToString(sum[:]) != webInstallerSHA {
		return errors.New("WEB proxy installer checksum mismatch")
	}
	installer = bytes.ReplaceAll(installer,
		[]byte("https://github.com/telegramdesktop/tproxy-server/archive/refs/heads/master.tar.gz"),
		[]byte("https://github.com/telegramdesktop/tproxy-server/archive/f7a6acc4d536a787d442fd7df3ba4ebfd728f406.tar.gz"))
	installer = bytes.ReplaceAll(installer,
		[]byte(`repo="$work/tproxy-server-master"`),
		[]byte(`repo="$work/tproxy-server-f7a6acc4d536a787d442fd7df3ba4ebfd728f406"`))
	installer = bytes.ReplaceAll(installer,
		[]byte(`sed -i 's/attempt != 20/attempt != 90/' "$repo/deploy/install.sh"`),
		[]byte("sed -i 's/attempt != 20/attempt != 90/' \"$repo/deploy/install.sh\"\n    sed -i '/install-mtproxy.sh/a\\chmod -R a+rX /opt/MTProxy' \"$repo/deploy/install.sh\""))
	if err := os.MkdirAll(m.dir, 0o700); err != nil {
		return err
	}
	installerPath := filepath.Join(m.dir, "tproxy-web-install.sh")
	if err := writeFileAtomic(installerPath, installer, 0o700); err != nil {
		return err
	}
	settings := inbounds[0].Settings
	siteMode, siteValue := "1", ""
	if upstream := stringSetting(settings, "site_upstream"); upstream != "" {
		siteMode, siteValue = "3", upstream
	}
	answers := []string{}
	if reinstall {
		answers = append(answers, "y")
	}
	answers = append(answers, stringSetting(settings, "hostname"))
	if dnsConfirmation {
		answers = append(answers, "y")
	}
	answers = append(answers,
		stringSetting(settings, "acme_email"),
		strings.TrimPrefix(strings.TrimPrefix(stringSetting(settings, "secret"), "dd"), "ee"),
		siteMode,
		siteValue,
	)
	input := strings.Join(answers, "\n") + "\n"
	logFile, err := os.OpenFile(filepath.Join(m.dir, "web-install.log"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	command := exec.Command("bash", installerPath, "install")
	command.Stdin = strings.NewReader(input)
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	m.webInstalling = true
	go func() {
		err := command.Wait()
		_ = logFile.Close()
		m.mu.Lock()
		defer m.mu.Unlock()
		m.webInstalling = false
		if err != nil {
			log.Printf("WEB proxy installation failed: %v", err)
			return
		}
		if err := writeFileAtomic(statePath, desired, 0o600); err != nil {
			log.Printf("failed to save WEB proxy state: %v", err)
		}
	}()
	return nil
}

func validateWebPlatform() error {
	raw, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return errors.New("WEB proxy requires Ubuntu 22.04+ or Debian 12+")
	}
	fields := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok {
			fields[key] = strings.Trim(strings.TrimSpace(value), `"'`)
		}
	}
	majorText, _, _ := strings.Cut(fields["VERSION_ID"], ".")
	major, _ := strconv.Atoi(majorText)
	if (fields["ID"] == "ubuntu" && major >= 22) || (fields["ID"] == "debian" && major >= 12) {
		return nil
	}
	return errors.New("WEB proxy requires Ubuntu 22.04+ or Debian 12+")
}

func webDNSNeedsConfirmation(hostname string) (bool, error) {
	resolvedOutput, err := exec.Command("getent", "ahostsv4", hostname).Output()
	if err != nil || len(strings.Fields(string(resolvedOutput))) == 0 {
		return false, errors.New("WEB proxy hostname must resolve before installation")
	}
	resolved := strings.Fields(string(resolvedOutput))[0]
	routeOutput, _ := exec.Command("ip", "-4", "route", "get", "1.1.1.1").Output()
	fields := strings.Fields(string(routeOutput))
	for index := 0; index+1 < len(fields); index++ {
		if fields[index] == "src" {
			return fields[index+1] != resolved, nil
		}
	}
	return false, nil
}

func (m *externalProxyManager) State(protocol string) (configured, running int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runtime != nil {
		for _, inbound := range m.runtime.Inbounds {
			if strings.EqualFold(inbound.Protocol, protocol) {
				configured++
			}
		}
	}
	if protocol == "mtproto" {
		for _, process := range m.mtProcesses {
			if process.Process != nil && processExists(process.Process.Pid) {
				running++
			}
		}
	} else if protocol == "web" && configured > 0 && exec.Command("systemctl", "is-active", "--quiet", "tproxy-server").Run() == nil {
		running = 1
	}
	return configured, running
}

func stringSetting(settings map[string]any, key string) string {
	value, _ := settings[key].(string)
	return strings.TrimSpace(value)
}

func boolSetting(settings map[string]any, key string) bool {
	value, _ := settings[key].(bool)
	return value
}
