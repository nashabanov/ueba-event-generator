package monitoring

import (
	"context"
	"log"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/metrics"
)

type Monitor interface {
	Start(ctx context.Context)
	Stop() error
}

// Monitor периодически выводит статистику
type MonitorImp struct {
	interval time.Duration
	metrics  *metrics.PerformanceMetrics
	stopChan chan struct{}
}

func NewMonitor(interval time.Duration) *MonitorImp {
	return &MonitorImp{
		interval: interval,
		metrics:  metrics.GetGlobalMetrics(),
		stopChan: make(chan struct{}),
	}
}

func (m *MonitorImp) Start(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	log.Printf("📊 Monitor запущен: интервал %v", m.interval)

	for {
		select {
		case <-ticker.C:
			log.Printf("%s", m.metrics.String())

		case <-ctx.Done():
			log.Printf("📊 Monitor остановлен")
			m.printFinalStats()
			return

		case <-m.stopChan:
			log.Printf("📊 Monitor остановлен (stop signal)")
			m.printFinalStats()
			return
		}
	}
}

func (m *MonitorImp) Stop() error {
	select {
	case <-m.stopChan:

	default:
		close(m.stopChan)
	}
	return nil
}

func (m *MonitorImp) printFinalStats() {
	log.Printf("=== ФИНАЛЬНАЯ СТАТИСТИКА ===")
	log.Printf("%s", m.metrics.String())
}
