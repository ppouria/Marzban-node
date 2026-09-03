//go:build !linux

package xray

func cleanupStaleManagedProcesses(executable string) {}
