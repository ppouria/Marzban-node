//go:build linux

package node

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/rebeccapanel/rebecca-node/internal/xray"
)

const accelRoot = "/usr/libexec/rebecca-accel"

func (m *extraVPNManager) Apply(runtimeConfig *extraRuntime) error {
	if m == nil || runtimeConfig == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	selected := extraRuntime{GeneratedAt: runtimeConfig.GeneratedAt, Target: runtimeConfig.Target, SessionCallback: runtimeConfig.SessionCallback, Inbounds: []extraRuntimeInbound{}}
	for _, inbound := range runtimeConfig.Inbounds {
		switch strings.ToLower(strings.TrimSpace(inbound.Protocol)) {
		case "sstp", "amneziawg", "gre":
			selected.Inbounds = append(selected.Inbounds, inbound)
		}
	}
	if len(selected.Inbounds) > 0 && m.installMode != "binary" {
		return errors.New("SSTP, AmneziaWG and GRE are supported only on binary Rebecca-node installs")
	}
	if err := os.MkdirAll(m.baseDir, 0o700); err != nil {
		return err
	}
	if raw, err := json.MarshalIndent(&selected, "", "  "); err != nil {
		return err
	} else if err := writeFileAtomic(filepath.Join(m.baseDir, "runtime.json"), raw, 0o600); err != nil {
		return err
	}
	if err := m.applySSTPLocked(filterExtraVPNInbounds(selected.Inbounds, "sstp"), selected.SessionCallback); err != nil {
		return err
	}
	if err := m.applyAWGLocked(filterExtraVPNInbounds(selected.Inbounds, "amneziawg"), selected.SessionCallback); err != nil {
		return err
	}
	if err := m.applyGRELocked(filterExtraVPNInbounds(selected.Inbounds, "gre"), selected.SessionCallback); err != nil {
		return err
	}
	m.runtime = &selected
	return nil
}

func filterExtraVPNInbounds(inbounds []extraRuntimeInbound, protocol string) []extraRuntimeInbound {
	result := []extraRuntimeInbound{}
	for _, inbound := range inbounds {
		if strings.EqualFold(inbound.Protocol, protocol) {
			result = append(result, inbound)
		}
	}
	return result
}

func (m *extraVPNManager) assetName(name string) (string, string) {
	asset := "rebecca-" + name + "-linux-" + runtime.GOARCH
	base := "https://github.com/rebeccapanel/rebecca-node/releases/latest/download/"
	if m.updateChannel != nil && m.updateChannel() == "dev" {
		asset = "rebecca-" + name + "-dev-linux-" + runtime.GOARCH
		base = "https://github.com/rebeccapanel/rebecca-node/releases/download/dev-binaries/"
	}
	return base, asset
}

func (m *extraVPNManager) installAsset(name, destination string, minSize, maxSize int) error {
	if fileExists(destination) {
		return nil
	}
	base, asset := m.assetName(name)
	body, err := download(base+asset, 10*time.Minute)
	if err != nil {
		return fmt.Errorf("download %s: %w", name, err)
	}
	checksum, err := download(base+asset+".sha256", 30*time.Second)
	if err != nil {
		return fmt.Errorf("download %s checksum: %w", name, err)
	}
	want := strings.Fields(string(checksum))
	sum := sha256.Sum256(body)
	if len(want) == 0 || !strings.EqualFold(want[0], hex.EncodeToString(sum[:])) {
		return fmt.Errorf("%s checksum mismatch", name)
	}
	if len(body) < minSize || len(body) > maxSize {
		return fmt.Errorf("%s has an unexpected size", name)
	}
	return writeFileAtomic(destination, body, 0o700)
}

func (m *extraVPNManager) installAccelBundle() error {
	bin := filepath.Join(accelRoot, "sbin", "accel-pppd")
	if fileExists(bin) {
		return nil
	}
	base, asset := m.assetName("accel-ppp")
	body, err := download(base+asset+".tar.gz", 10*time.Minute)
	if err != nil {
		return fmt.Errorf("download accel-ppp: %w", err)
	}
	checksum, err := download(base+asset+".tar.gz.sha256", 30*time.Second)
	if err != nil {
		return fmt.Errorf("download accel-ppp checksum: %w", err)
	}
	want := strings.Fields(string(checksum))
	sum := sha256.Sum256(body)
	if len(want) == 0 || !strings.EqualFold(want[0], hex.EncodeToString(sum[:])) {
		return errors.New("accel-ppp checksum mismatch")
	}
	if len(body) < 1<<20 || len(body) > 100<<20 {
		return errors.New("accel-ppp bundle has an unexpected size")
	}
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer reader.Close()
	archive := tar.NewReader(reader)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		clean := filepath.Clean("/" + header.Name)
		if !strings.HasPrefix(clean, accelRoot+string(os.PathSeparator)) && clean != accelRoot {
			return errors.New("accel-ppp archive contains an invalid path")
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(clean, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(clean), 0o755); err != nil {
				return err
			}
			if info, statErr := os.Lstat(clean); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
				return errors.New("accel-ppp destination contains a symbolic link")
			}
			file, err := os.OpenFile(clean, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(header.Mode)&0o755)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(file, archive, header.Size)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		}
	}
	if !fileExists(bin) {
		return errors.New("accel-ppp bundle does not contain accel-pppd")
	}
	return nil
}

func (m *extraVPNManager) collectUsageLocked() []xray.UserStat {
	stats := []xray.UserStat{}
	stats = append(stats, m.collectSSTPUsageLocked()...)
	stats = append(stats, m.collectAWGUsageLocked()...)
	stats = append(stats, m.collectGREUsageLocked()...)
	return stats
}
