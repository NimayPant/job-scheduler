package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func BenchmarkComputeBackoff(b *testing.B) {
	cfg := DefaultConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ComputeBackoff(cfg, i%5)
	}
}

func BenchmarkComputeBackoffMaxCapped(b *testing.B) {
	cfg := Config{
		MaxRetries:        20,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		BackoffMultiplier: 3.0,
		JitterFraction:    0.1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ComputeBackoff(cfg, 15)
	}
}

func BenchmarkIsTransient(b *testing.B) {
	transient := NewTransientError(errors.New("connection reset"))
	plain := errors.New("some error")

	b.Run("transient", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			IsTransient(transient)
		}
	})
	b.Run("plain", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			IsTransient(plain)
		}
	})
}

func BenchmarkIsPermanent(b *testing.B) {
	permanent := NewPermanentError(errors.New("invalid config"))
	plain := errors.New("some error")

	b.Run("permanent", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			IsPermanent(permanent)
		}
	})
	b.Run("plain", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			IsPermanent(plain)
		}
	})
}

func BenchmarkDoImmediateSuccess(b *testing.B) {
	cfg := Config{
		MaxRetries:        3,
		InitialBackoff:    time.Nanosecond,
		MaxBackoff:        time.Nanosecond,
		BackoffMultiplier: 1.0,
		JitterFraction:    0,
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Do(ctx, cfg, func(ctx context.Context, attempt int) error {
			return nil
		})
	}
}

func BenchmarkDoPermanentFailure(b *testing.B) {
	cfg := Config{
		MaxRetries:        5,
		InitialBackoff:    time.Nanosecond,
		MaxBackoff:        time.Nanosecond,
		BackoffMultiplier: 1.0,
		JitterFraction:    0,
	}
	ctx := context.Background()
	permErr := NewPermanentError(errors.New("bad request"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Do(ctx, cfg, func(ctx context.Context, attempt int) error {
			return permErr
		})
	}
}

func BenchmarkDoRetryThenSucceed(b *testing.B) {
	cfg := Config{
		MaxRetries:        3,
		InitialBackoff:    time.Nanosecond,
		MaxBackoff:        time.Nanosecond,
		BackoffMultiplier: 1.0,
		JitterFraction:    0,
	}
	ctx := context.Background()
	transientErr := NewTransientError(errors.New("timeout"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Do(ctx, cfg, func(ctx context.Context, attempt int) error {
			if attempt < 2 {
				return transientErr
			}
			return nil
		})
	}
}
