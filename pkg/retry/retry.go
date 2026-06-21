package retry

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

type TransientError struct {
	Err error
}

func (e *TransientError) Error() string { return e.Err.Error() }
func (e *TransientError) Unwrap() error { return e.Err }

func NewTransientError(err error) error {
	return &TransientError{Err: err}
}

func IsTransient(err error) bool {
	var te *TransientError
	return errors.As(err, &te)
}

type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

func NewPermanentError(err error) error {
	return &PermanentError{Err: err}
}

func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

type Config struct {
	MaxRetries        int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	JitterFraction    float64 // 0.0 to 1.0, default 0.25
}

func DefaultConfig() Config {
	return Config{
		MaxRetries:        3,
		InitialBackoff:    time.Second,
		MaxBackoff:        30 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.25,
	}
}

func ComputeBackoff(cfg Config, attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	backoff := float64(cfg.InitialBackoff) * math.Pow(cfg.BackoffMultiplier, float64(attempt))
	if backoff > float64(cfg.MaxBackoff) {
		backoff = float64(cfg.MaxBackoff)
	}

	jitter := cfg.JitterFraction
	if jitter <= 0 {
		jitter = 0.25
	}
	delta := backoff * jitter
	backoff = backoff - delta + (rand.Float64() * 2 * delta)

	return time.Duration(backoff)
}

func Do(ctx context.Context, cfg Config, fn func(ctx context.Context, attempt int) error) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		err := fn(ctx, attempt)
		if err == nil {
			return nil
		}
		lastErr = err

		if IsPermanent(err) {
			return err
		}

		if attempt >= cfg.MaxRetries {
			break
		}

		backoff := ComputeBackoff(cfg, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// Continue to next attempt.
		}
	}
	return lastErr
}
