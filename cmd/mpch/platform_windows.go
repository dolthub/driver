//go:build windows

package main

import (
	"os"
	"os/exec"
)

func setWorkerSysProcAttr(cmd *exec.Cmd) {
	// No Unix-style process groups / Setpgid on Windows.
	// Keep default behavior.
}

func interruptWorker(p *workerProc) {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	// Best-effort: on Windows os.Interrupt may not be supported for Process.Signal.
	// If unsupported, the subsequent kill escalation will terminate the process.
	_ = p.cmd.Process.Signal(os.Interrupt)
}

func killWorker(p *workerProc) bool {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return false
	}
	_ = p.cmd.Process.Kill()
	return true
}
