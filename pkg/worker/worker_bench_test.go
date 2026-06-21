package worker

import (
	"bytes"
	"testing"
)

func BenchmarkLimitedWriter(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data line\n"), 100)

	b.Run("under-limit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			w := &limitedWriter{buf: &buf, max: 1024 * 1024}
			w.Write(data)
		}
	})

	b.Run("at-limit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			w := &limitedWriter{buf: &buf, max: 500}
			for j := 0; j < 100; j++ {
				_, _ = w.Write(data)
			}
		}
	})

	b.Run("over-limit-discard", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			w := &limitedWriter{buf: &buf, max: 64}
			for j := 0; j < 1000; j++ {
				_, _ = w.Write(data)
			}
		}
	})
}

func BenchmarkLimitedWriterSmallChunks(b *testing.B) {
	chunk := []byte("x")

	b.Run("1byte-writes", func(b *testing.B) {
		var buf bytes.Buffer
		w := &limitedWriter{buf: &buf, max: 1024 * 1024}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			w.Write(chunk)
		}
	})
}
