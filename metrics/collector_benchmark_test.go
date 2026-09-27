package metrics

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkFilledObserveWindow(b *testing.B) {
	for _, window := range []int{1, defaultMaxLatencySamples} {
		b.Run(fmt.Sprint(window), func(b *testing.B) {
			c := New(WithMaxLatencySamples(window))
			for i := 0; i < window; i++ {
				c.ObserveRequest(time.Duration(i), true)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.ObserveRequest(time.Millisecond, true)
				}
			})
		})
	}
}

func BenchmarkMixedObserveAndSnapshot(b *testing.B) {
	for _, window := range []int{1, defaultMaxLatencySamples} {
		b.Run(fmt.Sprint(window), func(b *testing.B) {
			c := New(WithMaxLatencySamples(window))
			for i := 0; i < window; i++ {
				c.ObserveRequest(time.Duration((i*971)%window), true)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for i := 0; pb.Next(); i++ {
					if i%1000 == 0 {
						_ = c.Snapshot()
					} else {
						c.ObserveRequest(time.Millisecond, true)
					}
				}
			})
		})
	}
}
