package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// initialSampleCap is the initial capacity for latency sample storage.
	initialSampleCap = 4096
	// p99Percentile is the percentile threshold for p99 calculation.
	p99Percentile = 0.99
)

// StatsCollector tracks message counts and latency samples for periodic
// and final reporting.
type StatsCollector struct {
	published atomic.Int64
	consumed  atomic.Int64

	mu      sync.Mutex
	samples []int64 // latency samples in nanoseconds
	sampleN int     // sample every Nth consumed message

	start time.Time

	// snapshot fields for interval reporting
	lastPublished int64
	lastConsumed  int64
	lastTime      time.Time
	lastSamples   int
}

// NewStatsCollector returns a collector that samples latency every sampleN
// consumed messages.
func NewStatsCollector(sampleN int) *StatsCollector {
	now := time.Now()
	return &StatsCollector{
		sampleN:  sampleN,
		samples:  make([]int64, 0, initialSampleCap),
		start:    now,
		lastTime: now,
	}
}

// RecordPublish increments the published counter.
func (s *StatsCollector) RecordPublish() {
	s.published.Add(1)
}

// RecordConsume increments the consumed counter and optionally records a
// latency sample. The caller provides the end-to-end latency in
// nanoseconds. Sampling is performed on every sampleN-th message.
func (s *StatsCollector) RecordConsume(latencyNs int64) {
	n := s.consumed.Add(1)
	if s.sampleN > 0 && n%int64(s.sampleN) == 0 {
		s.mu.Lock()
		s.samples = append(s.samples, latencyNs)
		s.mu.Unlock()
	}
}

// Report returns a stats line for the current interval and resets the
// interval counters.
func (s *StatsCollector) Report() string {
	now := time.Now()
	pub := s.published.Load()
	con := s.consumed.Load()

	elapsed := now.Sub(s.lastTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}

	pubRate := float64(pub-s.lastPublished) / elapsed
	conRate := float64(con-s.lastConsumed) / elapsed

	s.mu.Lock()
	newSamples := s.samples[s.lastSamples:]
	avg, p99 := computeLatencyStats(newSamples)
	s.lastSamples = len(s.samples)
	s.mu.Unlock()

	s.lastPublished = pub
	s.lastConsumed = con
	s.lastTime = now

	interval := now.Sub(s.start).Truncate(time.Second)

	return fmt.Sprintf("time: %s, published: %.0f msg/s, consumed: %.0f msg/s, latency avg: %s p99: %s",
		interval, pubRate, conRate, formatDuration(avg), formatDuration(p99))
}

// Summary returns a final summary of the entire run.
func (s *StatsCollector) Summary(msgSize, publishers, consumers int) string {
	total := time.Since(s.start)
	pub := s.published.Load()
	con := s.consumed.Load()
	secs := total.Seconds()
	if secs <= 0 {
		secs = 1
	}

	s.mu.Lock()
	avg, p99 := computeLatencyStats(s.samples)
	s.mu.Unlock()

	var buf strings.Builder
	fmt.Fprintln(&buf, "SUMMARY:")
	fmt.Fprintf(&buf, "  Duration:    %.1fs\n", secs)
	fmt.Fprintf(&buf, "  Published:   %d messages (%.0f msg/s)\n", pub, float64(pub)/secs)
	fmt.Fprintf(&buf, "  Consumed:    %d messages (%.0f msg/s)\n", con, float64(con)/secs)
	fmt.Fprintf(&buf, "  Avg Latency: %s\n", formatDuration(avg))
	fmt.Fprintf(&buf, "  P99 Latency: %s\n", formatDuration(p99))
	fmt.Fprintf(&buf, "  Msg Size:    %d bytes\n", msgSize)
	fmt.Fprintf(&buf, "  Publishers:  %d\n", publishers)
	fmt.Fprintf(&buf, "  Consumers:   %d\n", consumers)
	return buf.String()
}

// Published returns the current published count.
func (s *StatsCollector) Published() int64 {
	return s.published.Load()
}

// Consumed returns the current consumed count.
func (s *StatsCollector) Consumed() int64 {
	return s.consumed.Load()
}

// computeLatencyStats returns average and p99 latencies from a slice of
// nanosecond samples. Returns zeros if the slice is empty.
func computeLatencyStats(samples []int64) (time.Duration, time.Duration) {
	if len(samples) == 0 {
		return 0, 0
	}

	sorted := make([]int64, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum int64
	for _, v := range sorted {
		sum += v
	}
	avg := time.Duration(sum / int64(len(sorted)))

	idx := int(float64(len(sorted)) * p99Percentile)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	p99 := time.Duration(sorted[idx])
	return avg, p99
}

// formatDuration formats a duration as a human-friendly millisecond string.
func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0.00ms"
	}
	ms := float64(d.Nanoseconds()) / float64(time.Millisecond)
	return fmt.Sprintf("%.2fms", ms)
}
