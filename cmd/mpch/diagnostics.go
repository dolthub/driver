package main

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func writeTimeoutDiagnostics(runPath string, manifest workerManifest, tailLines int, dumpStacks bool) error {
	diagDir := filepath.Join(runPath, "diagnostics")
	if err := os.MkdirAll(diagDir, 0o755); err != nil {
		return err
	}

	if dumpStacks {
		// Bound the stack dump size.
		buf := make([]byte, 1<<20) // 1 MiB
		n := runtime.Stack(buf, true)
		if err := os.WriteFile(filepath.Join(diagDir, "mpch_stacks.txt"), buf[:n], 0o644); err != nil {
			return err
		}
	}

	if tailLines <= 0 {
		return nil
	}

	workersOut := filepath.Join(diagDir, "workers")
	if err := os.MkdirAll(workersOut, 0o755); err != nil {
		return err
	}

	for _, w := range manifest.Workers {
		srcDir := filepath.Join(runPath, "workers", w.ID)
		dstDir := filepath.Join(workersOut, w.ID)
		if err := os.MkdirAll(dstDir, 0o755); err != nil {
			return err
		}

		_ = tailFile(filepath.Join(srcDir, "stdout.jsonl"), filepath.Join(dstDir, "stdout_tail.jsonl"), tailLines)
		_ = tailFile(filepath.Join(srcDir, "stderr.log"), filepath.Join(dstDir, "stderr_tail.log"), tailLines)

		if b, err := os.ReadFile(filepath.Join(srcDir, "termination.json")); err == nil {
			_ = os.WriteFile(filepath.Join(dstDir, "termination.json"), b, 0o644)
		}
	}

	return nil
}

func tailFile(src, dst string, n int) error {
	if n <= 0 {
		return nil
	}
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	type ringBuf struct {
		buf   []string
		next  int
		count int
	}
	r := ringBuf{buf: make([]string, n)}

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		r.buf[r.next] = sc.Text()
		r.next = (r.next + 1) % n
		if r.count < n {
			r.count++
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}

	var out strings.Builder
	start := 0
	if r.count == n {
		start = r.next
	}
	for i := 0; i < r.count; i++ {
		idx := (start + i) % n
		out.WriteString(r.buf[idx])
		out.WriteString("\n")
	}

	return os.WriteFile(dst, []byte(out.String()), 0o644)
}
