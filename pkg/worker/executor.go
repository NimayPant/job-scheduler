package worker

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"github.com/NimayPant/job-scheduler/pkg/models"
	"github.com/NimayPant/job-scheduler/pkg/retry"
)

type TaskResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Error    string
	Duration time.Duration
}

type Executor struct {
	maxOutputBytes int
}

func NewExecutor() *Executor {
	return &Executor{
		maxOutputBytes: 1024 * 1024, // 1MB max output capture
	}
}

func (e *Executor) Execute(ctx context.Context, task *models.Task) TaskResult {
	start := time.Now()

	cfg := retry.DefaultConfig()
	cfg.MaxRetries = task.MaxRetries

	var finalResult TaskResult

	err := retry.Do(ctx, cfg, func(ctx context.Context, attempt int) error {
		if attempt > 0 {
			log.Printf("retrying task %s (attempt %d/%d)", task.ID, attempt+1, task.MaxRetries+1)
		}

		result, execErr := e.executeOnce(ctx, task)
		finalResult = result

		if execErr != nil {
			// Check for transient vs permanent error.
			if retry.IsTransient(execErr) {
				return retry.NewTransientError(execErr)
			}
			return retry.NewPermanentError(execErr)
		}

		if result.ExitCode != 0 {
			return retry.NewTransientError(fmt.Errorf("non-zero exit code: %d", result.ExitCode))
		}

		return nil
	})

	finalResult.Duration = time.Since(start)
	if err != nil {
		finalResult.Error = err.Error()
	}
	return finalResult
}

func (e *Executor) executeOnce(ctx context.Context, task *models.Task) (TaskResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Hour)
	defer cancel()

	var stdout, stderr bytes.Buffer

	cmdName := task.Command
	args := task.Args

	// Windows shell wrapper
	if strings.Contains(cmdName, " ") || strings.ContainsAny(cmdName, "|>&") {
		args = append([]string{"/C", cmdName}, args...)
		cmdName = "cmd"
	}

	cmd := exec.CommandContext(ctx, cmdName, args...)
	cmd.Stdout = &limitedWriter{buf: &stdout, max: e.maxOutputBytes}
	cmd.Stderr = &limitedWriter{buf: &stderr, max: e.maxOutputBytes}

	err := cmd.Run()

	result := TaskResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		} else {
			result.ExitCode = -1
			result.Error = err.Error()
		}
		return result, err
	}

	result.ExitCode = 0
	return result, nil
}



type limitedWriter struct {
	buf     *bytes.Buffer
	max     int
	written int
}

func (w *limitedWriter) Write(p []byte) (n int, err error) {
	remaining := w.max - w.written
	if remaining <= 0 {
		return len(p), nil // Discard silently.
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	n, err = w.buf.Write(p)
	w.written += n
	return len(p), err // Report all bytes as "written" to avoid command failure.
}
