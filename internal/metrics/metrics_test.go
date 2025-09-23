package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestCounters(t *testing.T) {
	m := NewPerformanceMetrics()

	m.IncrementGenerated()
	m.IncrementSent()
	m.IncrementFailed()
	m.IncrementDropped()
	m.IncrementConnections()
	m.DecrementConnections()
	m.IncrementReconnects()
	m.IncrementTimeouts()

	// Делаем паузу >1 сек, чтобы EPS считался
	time.Sleep(1100 * time.Millisecond)
	m.IncrementGenerated() // теперь EPS > 0

	gen, sent, failed, eps := m.GetStats()
	if gen != 2 || sent != 1 || failed != 1 {
		t.Errorf("unexpected counters: %d %d %d", gen, sent, failed)
	}
	if eps <= 0 {
		t.Errorf("expected eps > 0, got %f", eps)
	}
}

func TestProcessingTime(t *testing.T) {
	m := NewPerformanceMetrics()

	m.IncrementGenerated() // чтобы avgProcessingTime считался
	m.RecordProcessingTime(10 * time.Millisecond)
	m.RecordProcessingTime(5 * time.Millisecond)

	stats := m.GetDetailedStats()
	if stats.MaxProcessingTime < stats.MinProcessingTime {
		t.Errorf("expected max >= min, got max=%v min=%v", stats.MaxProcessingTime, stats.MinProcessingTime)
	}
	if stats.AvgProcessingTime <= 0 {
		t.Errorf("expected avg > 0, got %v", stats.AvgProcessingTime)
	}
}

func TestCurrentEPS(t *testing.T) {
	m := NewPerformanceMetrics()

	m.IncrementGenerated()
	_ = m.calculateCurrentEPS()

	time.Sleep(1100 * time.Millisecond)

	m.IncrementGenerated()
	m.IncrementGenerated()

	eps := m.calculateCurrentEPS()
	if eps <= 0 {
		t.Errorf("expected current EPS > 0, got %f", eps)
	}
}

func TestStringOutput(t *testing.T) {
	m := NewPerformanceMetrics()
	out := m.String()
	if !strings.Contains(out, "Events:") || !strings.Contains(out, "Performance:") {
		t.Errorf("unexpected string output: %s", out)
	}
}

func TestGlobalMetrics(t *testing.T) {
	m1 := GetGlobalMetrics()
	m2 := GetGlobalMetrics()
	if m1 != m2 {
		t.Error("expected global metrics to be singleton")
	}
}
