//go:build !windows

package main

import (
	"os"
	"os/exec"
	"syscall"
)

func setWorkerSysProcAttr(cmd *exec.Cmd) {
	// Put the worker in its own process group so we can SIGKILL
	// the whole group on teardown if needed.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func interruptWorker(p *workerProc) {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
}

func killWorker(p *workerProc) bool {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return false
	}
	pid := p.cmd.Process.Pid
	// Kill the whole process group (negative pid).
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = p.cmd.Process.Kill()
	return true
}
