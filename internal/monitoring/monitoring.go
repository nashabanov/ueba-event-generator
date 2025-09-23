package monitoring

import (
	"context"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/logger"
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
	logger   logger.Logger
}

func NewMonitor(interval time.Duration, log logger.Logger) *MonitorImp {
	return &MonitorImp{
		interval: interval,
		metrics:  metrics.GetGlobalMetrics(),
		stopChan: make(chan struct{}),
		logger:   log,
	}
}

func (m *MonitorImp) Start(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	m.logger.Info("📊 Monitor запущен: интервал %v", m.interval)

	for {
		select {
		case <-ticker.C:
			m.logger.Info("%s", m.metrics.String())

		case <-ctx.Done():
			m.logger.Info("📊 Monitor остановлен")
			m.printFinalStats()
			return

		case <-m.stopChan:
			m.logger.Info("📊 Monitor остановлен (stop signal)")
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
	m.logger.Info("=== ФИНАЛЬНАЯ СТАТИСТИКА ===")
	m.logger.Info("%s", m.metrics.String())
}
