package monitoring

import (
	"context"
	"log"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/metrics"
)

// Monitor периодически выводит статистику
type Monitor struct {
	interval time.Duration
	metrics  *metrics.PerformanceMetrics
}

func NewMonitor(interval time.Duration) *Monitor {
	return &Monitor{
		interval: interval,
		metrics:  metrics.GetGlobalMetrics(),
	}
}

func (m *Monitor) Start(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	log.Printf("📊 Monitor запущен: интервал %v", m.interval)

	for {
		select {
		case <-ticker.C:
			// Выводим текущую статистику
			log.Printf("%s", m.metrics.String())

		case <-ctx.Done():
			log.Printf("📊 Monitor остановлен")
			// Финальная статистика
			log.Printf("=== ФИНАЛЬНАЯ СТАТИСТИКА ===")
			log.Printf("%s", m.metrics.String())
			return
		}
	}
}
