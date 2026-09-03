package node

import (
	"bufio"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type systemSampler struct {
	mu sync.Mutex

	lastCPU cpuTimes
	hasCPU  bool

	lastNet netTotals
	hasNet  bool
	lastAt  time.Time

	processPID   int
	processTicks uint64
	processAt    time.Time
}

type systemSnapshot struct {
	CPU       cpuSnapshot       `json:"cpu"`
	Memory    memorySnapshot    `json:"memory"`
	Bandwidth bandwidthSnapshot `json:"bandwidth"`
	UptimeSec uint64            `json:"uptime_seconds"`
}

type cpuSnapshot struct {
	Cores       int     `json:"cores"`
	FrequencyHz float64 `json:"frequency_hz"`
	UsagePct    float64 `json:"usage_percent"`
}

type memorySnapshot struct {
	UsedBytes  uint64  `json:"used_bytes"`
	TotalBytes uint64  `json:"total_bytes"`
	UsagePct   float64 `json:"usage_percent"`
}

type bandwidthSnapshot struct {
	UploadBytesPerSecond   uint64 `json:"upload_bytes_per_second"`
	DownloadBytesPerSecond uint64 `json:"download_bytes_per_second"`
}

type processSnapshot struct {
	PID         int
	CPUUsagePct float64
	MemoryUsed  uint64
	UptimeSec   uint64
}

type cpuTimes struct {
	total uint64
	idle  uint64
}

type netTotals struct {
	rx uint64
	tx uint64
}

func newSystemSampler() *systemSampler {
	return &systemSampler{}
}

func (s *systemSampler) Snapshot() systemSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	cpu := cpuSnapshot{
		Cores:       runtime.NumCPU(),
		FrequencyHz: readCPUFrequencyHz("/proc/cpuinfo"),
	}
	if times, ok := readCPUTimes("/proc/stat"); ok {
		if s.hasCPU {
			totalDelta := times.total - s.lastCPU.total
			idleDelta := times.idle - s.lastCPU.idle
			if totalDelta > 0 && idleDelta <= totalDelta {
				cpu.UsagePct = roundFloat(100*(1-float64(idleDelta)/float64(totalDelta)), 1)
			}
		}
		s.lastCPU = times
		s.hasCPU = true
	}

	memory := readMemorySnapshot("/proc/meminfo")

	bandwidth := bandwidthSnapshot{}
	if totals, ok := readNetworkTotals("/proc/net/dev"); ok {
		if s.hasNet && !s.lastAt.IsZero() {
			elapsed := now.Sub(s.lastAt).Seconds()
			if elapsed > 0 {
				if totals.tx >= s.lastNet.tx {
					bandwidth.UploadBytesPerSecond = uint64(float64(totals.tx-s.lastNet.tx) / elapsed)
				}
				if totals.rx >= s.lastNet.rx {
					bandwidth.DownloadBytesPerSecond = uint64(float64(totals.rx-s.lastNet.rx) / elapsed)
				}
			}
		}
		s.lastNet = totals
		s.hasNet = true
		s.lastAt = now
	}

	return systemSnapshot{
		CPU:       cpu,
		Memory:    memory,
		Bandwidth: bandwidth,
		UptimeSec: readUptimeSeconds("/proc/uptime"),
	}
}

func (s *systemSampler) ProcessSnapshot(pid int, uptime uint64) processSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := processSnapshot{PID: pid, UptimeSec: uptime}
	if pid <= 0 {
		s.processPID, s.processTicks, s.processAt = 0, 0, time.Time{}
		return result
	}
	now := time.Now()
	ticks, ok := readProcessTicks(pid)
	if !ok {
		s.processPID, s.processTicks, s.processAt = 0, 0, time.Time{}
		result.MemoryUsed = readProcessMemory(pid)
		return result
	}
	if ok && s.processPID == pid && !s.processAt.IsZero() && ticks >= s.processTicks {
		elapsed := now.Sub(s.processAt).Seconds()
		if elapsed > 0 {
			result.CPUUsagePct = roundFloat(float64(ticks-s.processTicks)/elapsed, 1)
		}
	}
	s.processPID, s.processTicks, s.processAt = pid, ticks, now
	result.MemoryUsed = readProcessMemory(pid)
	return result
}

func readProcessTicks(pid int) (uint64, bool) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}
	return parseProcessTicks(string(raw))
}

func parseProcessTicks(raw string) (uint64, bool) {
	end := strings.LastIndexByte(raw, ')')
	if end < 0 {
		return 0, false
	}
	fields := strings.Fields(raw[end+1:])
	if len(fields) <= 12 {
		return 0, false
	}
	user, errUser := strconv.ParseUint(fields[11], 10, 64)
	system, errSystem := strconv.ParseUint(fields[12], 10, 64)
	return user + system, errUser == nil && errSystem == nil
}

func readProcessMemory(pid int) uint64 {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		value, _ := strconv.ParseUint(fields[1], 10, 64)
		return value * 1024
	}
	return 0
}

func readUptimeSeconds(path string) uint64 {
	content, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(content))
	if len(fields) == 0 {
		return 0
	}
	seconds, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || seconds < 0 {
		return 0
	}
	return uint64(seconds)
}

func readCPUTimes(path string) (cpuTimes, bool) {
	file, err := os.Open(path)
	if err != nil {
		return cpuTimes{}, false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return cpuTimes{}, false
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuTimes{}, false
	}

	var total uint64
	var idle uint64
	for index, field := range fields[1:] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return cpuTimes{}, false
		}
		total += value
		if index == 3 || index == 4 {
			idle += value
		}
	}
	return cpuTimes{total: total, idle: idle}, true
}

func readCPUFrequencyHz(path string) float64 {
	file, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		key, value, ok := strings.Cut(line, ":")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "cpu MHz") {
			continue
		}
		mhz, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			return 0
		}
		return mhz * 1_000_000
	}
	return 0
}

func readMemorySnapshot(path string) memorySnapshot {
	file, err := os.Open(path)
	if err != nil {
		return memorySnapshot{}
	}
	defer file.Close()

	var total uint64
	var available uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch strings.TrimSuffix(fields[0], ":") {
		case "MemTotal":
			total = value * 1024
		case "MemAvailable":
			available = value * 1024
		}
	}

	if total == 0 {
		return memorySnapshot{}
	}
	used := total
	if available <= total {
		used = total - available
	}
	return memorySnapshot{
		UsedBytes:  used,
		TotalBytes: total,
		UsagePct:   roundFloat(float64(used)*100/float64(total), 1),
	}
}

func readNetworkTotals(path string) (netTotals, bool) {
	file, err := os.Open(path)
	if err != nil {
		return netTotals{}, false
	}
	defer file.Close()

	var totals netTotals
	scanner := bufio.NewScanner(file)
	for lineNumber := 0; scanner.Scan(); lineNumber++ {
		if lineNumber < 2 {
			continue
		}
		line := scanner.Text()
		iface, stats, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if strings.TrimSpace(iface) == "lo" {
			continue
		}
		fields := strings.Fields(stats)
		if len(fields) < 16 {
			continue
		}
		rx, rxErr := strconv.ParseUint(fields[0], 10, 64)
		tx, txErr := strconv.ParseUint(fields[8], 10, 64)
		if rxErr != nil || txErr != nil {
			continue
		}
		totals.rx += rx
		totals.tx += tx
	}
	return totals, true
}

func roundFloat(value float64, precision int) float64 {
	if precision < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return value
	}
	multiplier := math.Pow10(precision)
	return math.Round(value*multiplier) / multiplier
}
