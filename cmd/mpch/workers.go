package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func runWorkersAndTicks(
	ctx context.Context,
	cfg harnessConfig,
	runID, runPath string,
	manifest workerManifest,
	emit func(phase, name string, fields any),
) (retErr error) {
	workerBin, err := exec.LookPath(cfg.WorkerBin)
	if err != nil {
		return fmt.Errorf("failed to find worker binary %q: %w", cfg.WorkerBin, err)
	}

	procs := make([]*workerProc, 0, len(manifest.Workers))
	defer func() {
		if err := shutdownWorkers(procs, cfg.WorkerShutdownGrace, cfg.WorkerShutdownKillWait, emit); err != nil {
			if retErr == nil {
				retErr = err
			} else {
				retErr = fmt.Errorf("%v; shutdown error: %w", retErr, err)
			}
		}
	}()

	for _, w := range manifest.Workers {
		p, err := startWorker(ctx, workerBin, runID, runPath, w, workerLaunchArgs{
			Mode:            cfg.WorkerMode,
			DSN:             cfg.DSN,
			DB:              cfg.WorkerDB,
			Table:           cfg.WorkerTable,
			OpInterval:      cfg.WorkerOpInterval,
			OpenRetry:       cfg.WorkerOpenRetry,
			SQLSession:      cfg.WorkerSQLSession,
			OpTimeout:       cfg.WorkerOpTimeout,
			Heartbeat:       cfg.WorkerHeartbeatInterval,
			IgnoreInterrupt: cfg.WorkerIgnoreInterrupt,
		}, emit)
		if err != nil {
			return err
		}
		procs = append(procs, p)
	}
	emit("run", "workers_spawned", map[string]any{"count": len(procs)})

	// Ready barrier.
	for _, p := range procs {
		select {
		case <-p.readyCh:
		case <-p.doneCh:
			err := workerWaitErr(p)
			if err == nil {
				return fmt.Errorf("worker %s exited before ready", p.spec.ID)
			}
			return fmt.Errorf("worker %s exited before ready: %w", p.spec.ID, err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	emit("run", "all_ready", map[string]any{"count": len(procs)})

	// Start signal.
	for _, p := range procs {
		if _, err := io.WriteString(p.stdin, "start\n"); err != nil {
			return fmt.Errorf("failed to send start to worker %s: %w", p.spec.ID, err)
		}
	}
	emit("run", "all_started", map[string]any{"count": len(procs)})

	// Watch for workers exiting early.
	exitNotify := make(chan workerExit, len(procs))
	for _, p := range procs {
		pp := p
		go func() {
			<-pp.doneCh
			err := workerWaitErr(pp)
			we := workerExit{WorkerID: pp.spec.ID, Role: pp.spec.Role}
			if err != nil {
				we.Error = err.Error()
			}
			exitNotify <- we
		}()
	}

	// Run ticks for duration (or until cancel), failing fast on worker exit.
	runErr := runWithTicksOrWorkerExit(ctx, cfg.Duration, cfg.TickInterval, emit, exitNotify)
	// cleanup() will stop workers.
	return runErr
}

func startWorker(
	ctx context.Context,
	workerBin string,
	runID string,
	runPath string,
	spec workerSpec,
	args workerLaunchArgs,
	emit func(phase, name string, fields any),
) (*workerProc, error) {
	workerDir := filepath.Join(runPath, "workers", spec.ID)
	if err := os.MkdirAll(workerDir, 0o755); err != nil {
		return nil, err
	}

	argv := []string{
		"--run-id", runID,
		"--worker-id", spec.ID,
		"--role", string(spec.Role),
		"--heartbeat-interval", args.Heartbeat.String(),
		"--wait-for-start=true",
		"--mode", args.Mode,
		"--dsn", args.DSN,
		"--db", args.DB,
		"--table", args.Table,
		"--op-interval", args.OpInterval.String(),
		"--open-retry=" + fmt.Sprintf("%t", args.OpenRetry),
		"--sql-session", args.SQLSession,
		"--op-timeout", args.OpTimeout.String(),
	}
	if args.IgnoreInterrupt {
		argv = append(argv, "--ignore-interrupt=true")
	}

	cmd := exec.CommandContext(ctx, workerBin, argv...)
	setWorkerSysProcAttr(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	stdoutFile, err := os.Create(filepath.Join(workerDir, "stdout.jsonl"))
	if err != nil {
		return nil, err
	}
	stderrFile, err := os.Create(filepath.Join(workerDir, "stderr.log"))
	if err != nil {
		_ = stdoutFile.Close()
		return nil, err
	}

	p := &workerProc{
		spec:      spec,
		cmd:       cmd,
		stdin:     stdin,
		workerDir: workerDir,
		readyCh:   make(chan struct{}),
		doneCh:    make(chan struct{}),
	}

	go func() {
		defer func() {
			_ = stdoutFile.Close()
		}()
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			_, _ = stdoutFile.WriteString(line + "\n")

			// Parse ready event.
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err == nil {
				if ev, _ := m["event"].(string); ev == "ready" {
					select {
					case <-p.readyCh:
					default:
						close(p.readyCh)
						emit("run", "worker_ready", map[string]any{"worker_id": spec.ID, "role": spec.Role})
					}
				}
			}
		}
	}()

	go func() {
		defer func() { _ = stderrFile.Close() }()
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			_, _ = stderrFile.WriteString(sc.Text() + "\n")
		}
	}()

	if err := cmd.Start(); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		return nil, err
	}
	emit("run", "worker_spawned", map[string]any{"worker_id": spec.ID, "role": spec.Role})

	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		p.waitErr = err
		p.mu.Unlock()
		close(p.doneCh)
	}()

	return p, nil
}

func workerWaitErr(p *workerProc) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

func shutdownWorkers(procs []*workerProc, grace, killWait time.Duration, emit func(phase, name string, fields any)) error {
	if len(procs) == 0 {
		return nil
	}

	// Close stdin (best-effort) to avoid hanging on input.
	for _, p := range procs {
		if p.stdin != nil {
			_ = p.stdin.Close()
		}
	}

	// First: SIGINT.
	for _, p := range procs {
		interruptWorker(p)
	}
	emit("run", "workers_interrupt_sent", map[string]any{"count": len(procs)})

	deadline := time.NewTimer(grace)
	defer deadline.Stop()

	done := make(map[*workerProc]struct{}, len(procs))
	for len(done) < len(procs) {
		progress := false
		for _, p := range procs {
			if _, ok := done[p]; ok {
				continue
			}
			select {
			case <-p.doneCh:
				done[p] = struct{}{}
				progress = true
			default:
			}
		}
		if progress {
			continue
		}
		select {
		case <-deadline.C:
			goto KILL
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	emit("run", "workers_exited", map[string]any{"count": len(procs), "killed": 0})
	for p := range done {
		err := workerWaitErr(p)
		_ = writeTermination(p, workerTermination{
			TS:        time.Now().UTC(),
			WorkerID:  p.spec.ID,
			Role:      p.spec.Role,
			Reason:    "graceful_exit",
			Signal:    "SIGINT",
			Killed:    false,
			ExitError: exitErrString(err),
		})
	}
	return nil

KILL:
	// Escalate: force kill.
	killed := 0
	for _, p := range procs {
		if _, ok := done[p]; ok {
			continue
		}
		if killWorker(p) {
			killed++
		}
	}
	emit("run", "workers_kill_sent", map[string]any{"count": killed})

	killDeadline := time.NewTimer(killWait)
	defer killDeadline.Stop()
	for len(done) < len(procs) {
		progress := false
		for _, p := range procs {
			if _, ok := done[p]; ok {
				continue
			}
			select {
			case <-p.doneCh:
				done[p] = struct{}{}
				progress = true
			default:
			}
		}
		if progress {
			continue
		}
		select {
		case <-killDeadline.C:
			emit("run", "workers_kill_timeout", map[string]any{"remaining": len(procs) - len(done)})
			for _, p := range procs {
				if _, ok := done[p]; ok {
					continue
				}
				_ = writeTermination(p, workerTermination{
					TS:       time.Now().UTC(),
					WorkerID: p.spec.ID,
					Role:     p.spec.Role,
					Reason:   "kill_timeout",
					Signal:   "SIGKILL",
					Killed:   true,
				})
			}
			return fmt.Errorf("timeout waiting for %d worker(s) to exit after SIGKILL", len(procs)-len(done))
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	emit("run", "workers_exited", map[string]any{"count": len(procs), "killed": killed})
	for _, p := range procs {
		err := workerWaitErr(p)
		term := workerTermination{
			TS:        time.Now().UTC(),
			WorkerID:  p.spec.ID,
			Role:      p.spec.Role,
			Killed:    false,
			ExitError: exitErrString(err),
		}
		if err == nil {
			term.Reason = "graceful_exit"
			term.Signal = "SIGINT"
		} else if killed > 0 {
			term.Reason = "killed_after_grace_timeout"
			term.Signal = "SIGKILL"
			term.Killed = true
		} else {
			term.Reason = "exited_with_error"
		}
		_ = writeTermination(p, term)
	}
	return fmt.Errorf("one or more workers required SIGKILL after grace timeout")
}

func writeTermination(p *workerProc, term workerTermination) error {
	if p == nil || p.workerDir == "" {
		return nil
	}
	b, err := json.MarshalIndent(term, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(p.workerDir, "termination.json"), b, 0o644)
}

func exitErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
