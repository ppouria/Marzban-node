package node

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	psiphonAccount       = "rebecca-psiphon"
	psiphonBinaryDir     = "/usr/local/lib/rebecca-psiphon"
	psiphonBinaryPath    = psiphonBinaryDir + "/psiphon-tunnel-core"
	psiphonConfigDir     = "/etc/rebecca/psiphon"
	psiphonDataDir       = "/var/lib/rebecca-psiphon"
	psiphonBinaryURL     = "https://raw.githubusercontent.com/Psiphon-Labs/psiphon-tunnel-core-binaries/master/linux/psiphon-tunnel-core-x86_64"
	psiphonMaxConfigSize = 1 << 20
)

var (
	psiphonCountryPattern = regexp.MustCompile(`^[a-zA-Z]{2}$`)
	psiphonSetupMu        sync.Mutex
)

type psiphonProxyConfig struct {
	Action     string
	ConfigJSON string
	Locations  []string
	SocksPort  uint32
}

type psiphonProxyInstance struct {
	Location  string
	SocksPort uint32
}

type psiphonProxyResult struct {
	Instances []psiphonProxyInstance
	Locations []string
}

func configurePsiphon(ctx context.Context, config psiphonProxyConfig) (psiphonProxyResult, error) {
	action := strings.ToLower(strings.TrimSpace(config.Action))
	if action == "" {
		action = "apply"
	}
	if action != "locations" && action != "apply" {
		return psiphonProxyResult{}, fmt.Errorf("Psiphon action must be locations or apply")
	}

	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return psiphonProxyResult{}, fmt.Errorf("Psiphon automatic setup is supported on Linux amd64 nodes only")
	}
	if os.Geteuid() != 0 {
		return psiphonProxyResult{}, fmt.Errorf("Psiphon automatic setup requires root")
	}
	if action == "locations" {
		locations, err := psiphonAvailableLocations(ctx, config.ConfigJSON)
		return psiphonProxyResult{Locations: locations}, err
	}

	psiphonSetupMu.Lock()
	defer psiphonSetupMu.Unlock()

	if !commandExists("systemctl") {
		return psiphonProxyResult{}, fmt.Errorf("Psiphon setup requires systemd")
	}
	locations, err := normalizedPsiphonLocations(config.Locations)
	if err != nil {
		return psiphonProxyResult{}, err
	}
	if config.SocksPort < 1024 || uint64(config.SocksPort)+uint64(len(locations))-1 > 65535 {
		return psiphonProxyResult{}, fmt.Errorf("Psiphon SOCKS ports must be between 1024 and 65535")
	}
	if _, err := psiphonConfigContents(config.ConfigJSON, locations[0], config.SocksPort); err != nil {
		return psiphonProxyResult{}, err
	}
	if err := ensurePsiphonInstalled(ctx); err != nil {
		return psiphonProxyResult{}, err
	}
	account, err := ensurePsiphonAccount(ctx)
	if err != nil {
		return psiphonProxyResult{}, err
	}

	instances := make([]psiphonProxyInstance, 0, len(locations))
	for index, location := range locations {
		port := config.SocksPort + uint32(index)
		if err := applyPsiphonInstance(ctx, account, config.ConfigJSON, location, port); err != nil {
			return psiphonProxyResult{}, err
		}
		instances = append(instances, psiphonProxyInstance{Location: location, SocksPort: port})
	}
	for _, instance := range instances {
		if err := waitForLocalPort(int(instance.SocksPort), 30*time.Second); err != nil {
			return psiphonProxyResult{}, fmt.Errorf("Psiphon SOCKS proxy did not start: %w", err)
		}
	}
	return psiphonProxyResult{Instances: instances}, nil
}

func normalizedPsiphonLocations(values []string) ([]string, error) {
	locations := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		location := strings.ToLower(strings.TrimSpace(value))
		if !psiphonCountryPattern.MatchString(location) {
			return nil, fmt.Errorf("Psiphon locations must be two-letter ISO codes")
		}
		if _, ok := seen[location]; ok {
			return nil, fmt.Errorf("Psiphon locations must be unique")
		}
		seen[location] = struct{}{}
		locations = append(locations, location)
	}
	if len(locations) == 0 {
		return nil, fmt.Errorf("at least one Psiphon location is required")
	}
	return locations, nil
}

func psiphonConfigContents(raw, location string, port uint32) ([]byte, error) {
	config, err := parsePsiphonConfig(raw)
	if err != nil {
		return nil, err
	}
	config["EgressRegion"] = strings.ToUpper(location)
	config["LocalSocksProxyPort"] = port
	config["DisableLocalSocksProxy"] = false
	config["DisableLocalHTTPProxy"] = true
	config["LocalHttpProxyPort"] = 0
	return json.MarshalIndent(config, "", "  ")
}

func psiphonAvailableLocations(ctx context.Context, rawConfig string) ([]string, error) {
	contents, err := psiphonDiscoveryConfigContents(rawConfig)
	if err != nil {
		return nil, err
	}
	if err := ensurePsiphonInstalled(ctx); err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp("", "rebecca-psiphon-locations-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, err
	}
	configPath := filepath.Join(directory, "config.json")
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	command := exec.CommandContext(runCtx, psiphonBinaryPath, "-config", configPath, "-dataRootDirectory", directory, "-listenInterface", "127.0.0.1")
	stderr, err := command.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start Psiphon location discovery: %w", err)
	}
	locationsResult := make(chan []string, 1)
	readResult := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stderr)
		scanner.Buffer(make([]byte, 1024), 256<<10)
		for scanner.Scan() {
			if locations, reported := parsePsiphonAvailableRegions(scanner.Bytes()); reported {
				locationsResult <- locations
				return
			}
		}
		readResult <- scanner.Err()
	}()

	select {
	case locations := <-locationsResult:
		cancel()
		_ = command.Wait()
		if len(locations) == 0 {
			return nil, fmt.Errorf("Psiphon did not report any available exit regions")
		}
		return locations, nil
	case err := <-readResult:
		waitErr := command.Wait()
		if err != nil {
			return nil, fmt.Errorf("read Psiphon location discovery: %w", err)
		}
		if waitErr != nil {
			return nil, fmt.Errorf("Psiphon location discovery failed: %w", waitErr)
		}
		return nil, fmt.Errorf("Psiphon did not report any available exit regions")
	case <-runCtx.Done():
		_ = command.Wait()
		return nil, fmt.Errorf("Psiphon location discovery timed out")
	}
}

func parsePsiphonConfig(raw string) (map[string]any, error) {
	if len(raw) == 0 || len(raw) > psiphonMaxConfigSize {
		return nil, fmt.Errorf("Psiphon config must be a JSON object no larger than 1 MB")
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(raw), &config); err != nil || config == nil {
		return nil, fmt.Errorf("Psiphon config must be a JSON object")
	}
	for _, field := range []string{"PropagationChannelId", "SponsorId"} {
		value, ok := config[field].(string)
		if !ok || strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("Psiphon config requires %s", field)
		}
	}
	return config, nil
}

func psiphonDiscoveryConfigContents(raw string) ([]byte, error) {
	config, err := parsePsiphonConfig(raw)
	if err != nil {
		return nil, err
	}
	delete(config, "EgressRegion")
	config["DisableLocalSocksProxy"] = true
	config["DisableLocalHTTPProxy"] = true
	config["DisableServerEntriesReporter"] = false
	config["DisableTunnels"] = false
	config["EnableLightProxyFallback"] = false
	config["LocalSocksProxyPort"] = 0
	config["LocalHttpProxyPort"] = 0
	return json.MarshalIndent(config, "", "  ")
}

func parsePsiphonAvailableRegions(notice []byte) ([]string, bool) {
	var payload struct {
		NoticeType string `json:"noticeType"`
		Data       struct {
			Regions []string `json:"regions"`
		} `json:"data"`
	}
	if json.Unmarshal(notice, &payload) != nil || payload.NoticeType != "AvailableEgressRegions" {
		return nil, false
	}
	regions := make([]string, 0, len(payload.Data.Regions))
	seen := make(map[string]struct{}, len(payload.Data.Regions))
	for _, value := range payload.Data.Regions {
		region := strings.ToLower(strings.TrimSpace(value))
		if !psiphonCountryPattern.MatchString(region) {
			continue
		}
		if _, exists := seen[region]; exists {
			continue
		}
		seen[region] = struct{}{}
		regions = append(regions, region)
	}
	sort.Strings(regions)
	return regions, true
}

func ensurePsiphonInstalled(ctx context.Context) error {
	if fileExists(psiphonBinaryPath) {
		return nil
	}
	body, err := download(psiphonBinaryURL, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("download Psiphon client: %w", err)
	}
	if len(body) < 1<<20 || len(body) > 32<<20 {
		return fmt.Errorf("download Psiphon client: unexpected binary size")
	}
	if err := os.MkdirAll(psiphonBinaryDir, 0o755); err != nil {
		return err
	}
	file, err := os.CreateTemp(psiphonBinaryDir, ".psiphon-*")
	if err != nil {
		return err
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(0o755); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, psiphonBinaryPath); err != nil {
		return err
	}
	return nil
}

func ensurePsiphonAccount(ctx context.Context) (*user.User, error) {
	account, err := user.Lookup(psiphonAccount)
	if err == nil {
		return account, nil
	}
	if err := runCommandContext(ctx, nil, "useradd", "--system", "--home-dir", psiphonDataDir, "--shell", "/usr/sbin/nologin", "--no-create-home", psiphonAccount); err != nil {
		return nil, fmt.Errorf("create Psiphon service account: %w", err)
	}
	return user.Lookup(psiphonAccount)
}

func applyPsiphonInstance(ctx context.Context, account *user.User, rawConfig, location string, port uint32) error {
	contents, err := psiphonConfigContents(rawConfig, location, port)
	if err != nil {
		return err
	}
	uid, err := strconv.Atoi(account.Uid)
	if err != nil {
		return err
	}
	gid, err := strconv.Atoi(account.Gid)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("rebecca-psiphon-%s-%d", location, port)
	configPath := filepath.Join(psiphonConfigDir, name+".json")
	dataPath := filepath.Join(psiphonDataDir, name)
	unitPath := filepath.Join("/etc/systemd/system", name+".service")
	if err := os.MkdirAll(psiphonConfigDir, 0o750); err != nil {
		return err
	}
	if err := os.Chown(psiphonConfigDir, uid, gid); err != nil {
		return err
	}
	if err := os.Chmod(psiphonConfigDir, 0o750); err != nil {
		return err
	}
	if err := os.MkdirAll(psiphonDataDir, 0o750); err != nil {
		return err
	}
	if err := os.Chown(psiphonDataDir, uid, gid); err != nil {
		return err
	}
	if err := os.Chmod(psiphonDataDir, 0o750); err != nil {
		return err
	}
	if err := os.MkdirAll(dataPath, 0o700); err != nil {
		return err
	}
	if err := os.Chown(dataPath, uid, gid); err != nil {
		return err
	}
	if err := os.Chmod(dataPath, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		return err
	}
	if err := os.Chown(configPath, uid, gid); err != nil {
		return err
	}
	if err := os.WriteFile(unitPath, []byte(psiphonSystemdUnit(account.Username, configPath, dataPath)), 0o644); err != nil {
		return err
	}
	if err := runCommandContext(ctx, nil, "systemctl", "daemon-reload"); err != nil {
		return fmt.Errorf("reload Psiphon units: %w", err)
	}
	if err := runCommandContext(ctx, nil, "systemctl", "enable", name+".service"); err != nil {
		return fmt.Errorf("enable Psiphon proxy: %w", err)
	}
	if err := runCommandContext(ctx, nil, "systemctl", "restart", name+".service"); err != nil {
		return fmt.Errorf("start Psiphon proxy: %w", err)
	}
	return nil
}

func psiphonSystemdUnit(account, configPath, dataPath string) string {
	return fmt.Sprintf(`[Unit]
Description=Rebecca Psiphon SOCKS proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=%s
Group=%s
ExecStart=%s -config %s -dataRootDirectory %s -listenInterface 127.0.0.1 -formatNotices
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadWritePaths=%s
UMask=0077

[Install]
WantedBy=multi-user.target
`, account, account, psiphonBinaryPath, configPath, dataPath, dataPath)
}
