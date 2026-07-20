package node

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"
)

const torRebeccaBlockStart = "# BEGIN REBECCA TOR PROXY"
const torRebeccaBlockEnd = "# END REBECCA TOR PROXY"

var torCountryCodePattern = regexp.MustCompile(`^[a-zA-Z]{2}$`)

type torProxyConfig struct {
	SocksPort   uint32
	ExitCountry string
	StrictExit  bool
}

func applyTorProxy(config torProxyConfig) error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("Tor proxy setup is supported on Linux nodes only")
	}
	if config.SocksPort < 1024 || config.SocksPort > 65535 {
		return fmt.Errorf("Tor SOCKS port must be between 1024 and 65535")
	}
	country := strings.ToLower(strings.TrimSpace(config.ExitCountry))
	if country != "" && !torCountryCodePattern.MatchString(country) {
		return fmt.Errorf("Tor exit country must be a two-letter ISO code")
	}
	if err := ensureTorInstalled(); err != nil {
		return err
	}
	if country != "" {
		if err := ensureTorGeoIPInstalled(); err != nil {
			return err
		}
	}
	if err := writeRebeccaTorConfig(config.SocksPort, country, config.StrictExit); err != nil {
		return err
	}
	if tor, err := exec.LookPath("tor"); err == nil {
		if output, err := exec.Command(tor, "--verify-config").CombinedOutput(); err != nil {
			return fmt.Errorf("verify tor config: %v: %s", err, strings.TrimSpace(string(output)))
		}
	}
	if err := restartTorService(); err != nil {
		return err
	}
	if err := waitForLocalPort(int(config.SocksPort), 20*time.Second); err != nil {
		return err
	}
	return waitForTorSocksReady(int(config.SocksPort), 90*time.Second)
}

func ensureTorInstalled() error {
	if _, err := exec.LookPath("tor"); err == nil {
		return nil
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("Tor is missing and automatic install requires root")
	}
	switch {
	case commandExists("apt-get"):
		if commandExists("dpkg") {
			_ = runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "dpkg", "--configure", "-a")
		}
		if err := runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "update"); err != nil {
			return err
		}
		return runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "install", "-y", "--no-install-recommends", "tor", "ca-certificates")
	case commandExists("dnf"):
		return runInstallCommand(nil, "dnf", "install", "-y", "tor")
	case commandExists("yum"):
		return runInstallCommand(nil, "yum", "install", "-y", "tor")
	case commandExists("apk"):
		return runInstallCommand(nil, "apk", "add", "tor")
	default:
		return fmt.Errorf("Tor is missing and no supported package manager was found")
	}
}

func ensureTorGeoIPInstalled() error {
	if fileExists("/usr/share/tor/geoip") && fileExists("/usr/share/tor/geoip6") {
		return nil
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("Tor GeoIP data is missing and automatic install requires root")
	}
	switch {
	case commandExists("apt-get"):
		if err := runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "update"); err != nil {
			return err
		}
		return runInstallCommand([]string{"DEBIAN_FRONTEND=noninteractive"}, "apt-get", "install", "-y", "--no-install-recommends", "tor-geoipdb")
	case commandExists("dnf"):
		return runInstallCommand(nil, "dnf", "install", "-y", "tor-geoipdb")
	case commandExists("yum"):
		return runInstallCommand(nil, "yum", "install", "-y", "tor-geoipdb")
	default:
		return fmt.Errorf("Tor GeoIP data is missing and no supported package manager was found")
	}
}

func writeRebeccaTorConfig(port uint32, country string, strict bool) error {
	path := "/etc/tor/torrc"
	raw, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	lines := []string{
		torRebeccaBlockStart,
		"SocksPort 0",
		fmt.Sprintf("SocksPort 127.0.0.1:%d", port),
		"ClientOnly 1",
		"AvoidDiskWrites 1",
	}
	if country != "" {
		lines = append(lines, fmt.Sprintf("ExitNodes {%s}", country))
		if strict {
			lines = append(lines, "StrictNodes 1")
		}
	}
	lines = append(lines, torRebeccaBlockEnd)
	next := replaceMarkedBlock(string(raw), torRebeccaBlockStart, torRebeccaBlockEnd, strings.Join(lines, "\n"))
	if err := os.MkdirAll("/etc/tor", 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(next), 0o644)
}

func replaceMarkedBlock(current, start, end, block string) string {
	current = strings.ReplaceAll(current, "\r\n", "\n")
	startIndex := strings.Index(current, start)
	endIndex := strings.Index(current, end)
	if startIndex >= 0 && endIndex >= startIndex {
		endIndex += len(end)
		current = strings.TrimSpace(current[:startIndex] + current[endIndex:])
	}
	if strings.TrimSpace(current) == "" {
		return block + "\n"
	}
	return strings.TrimRight(current, "\n") + "\n\n" + block + "\n"
}

func restartTorService() error {
	switch {
	case commandExists("systemctl"):
		if output, err := exec.Command("systemctl", "restart", "tor").CombinedOutput(); err == nil {
			return nil
		} else if output2, err2 := exec.Command("systemctl", "restart", "tor@default").CombinedOutput(); err2 != nil {
			return fmt.Errorf("restart tor service: %v: %s; %v: %s", err, strings.TrimSpace(string(output)), err2, strings.TrimSpace(string(output2)))
		}
		return nil
	case commandExists("service"):
		output, err := exec.Command("service", "tor", "restart").CombinedOutput()
		if err != nil {
			return fmt.Errorf("restart tor service: %v: %s", err, strings.TrimSpace(string(output)))
		}
		return nil
	default:
		return fmt.Errorf("no supported service manager found for Tor")
	}
}

func waitForLocalPort(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	address := net.JoinHostPort("127.0.0.1", fmt.Sprint(port))
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("Tor SOCKS proxy did not start on %s: %v", address, lastErr)
}

func waitForTorSocksReady(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := testSocks5Connect(port, "example.com", 80); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("Tor SOCKS proxy is not ready on 127.0.0.1:%d: %v", port, lastErr)
}

func testSocks5Connect(port int, host string, targetPort uint16) error {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprint(port)), 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	r := bufio.NewReader(conn)
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return err
	}
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}
	if header[0] != 0x05 || header[1] != 0x00 {
		return fmt.Errorf("SOCKS auth rejected")
	}
	hostBytes := []byte(host)
	if len(hostBytes) > 255 {
		return fmt.Errorf("SOCKS host is too long")
	}
	request := []byte{0x05, 0x01, 0x00, 0x03, byte(len(hostBytes))}
	request = append(request, hostBytes...)
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, targetPort)
	request = append(request, portBytes...)
	if _, err := conn.Write(request); err != nil {
		return err
	}
	response := make([]byte, 4)
	if _, err := io.ReadFull(r, response); err != nil {
		return err
	}
	if response[1] != 0x00 {
		return fmt.Errorf("SOCKS connect failed with code %d", response[1])
	}
	switch response[3] {
	case 0x01:
		_, err = io.CopyN(io.Discard, r, 6)
	case 0x03:
		length, err := r.ReadByte()
		if err != nil {
			return err
		}
		_, err = io.CopyN(io.Discard, r, int64(length)+2)
	case 0x04:
		_, err = io.CopyN(io.Discard, r, 18)
	default:
		err = fmt.Errorf("SOCKS response has invalid address type %d", response[3])
	}
	return err
}
