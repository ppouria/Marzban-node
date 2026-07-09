//go:build windows

package xray

import "os/exec"

func configureManagedProcess(cmd *exec.Cmd) {}

func terminateManagedProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}
