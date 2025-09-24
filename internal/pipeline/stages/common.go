package stages

import (
	"time"
)

// StageConfig базовая конфигурация для всех стадий
type StageConfig struct {
	Name        string `yaml:"name" json:"name"`
	WorkerCount int    `yaml:"worker_count" json:"worker_count"`
	BufferSize  int    `yaml:"buffer_size" json:"buffer_size"`
}

// StageMetrics базовые метрики стадии
type StageMetrics struct {
	Name            string    `json:"name"`
	ProcessedEvents uint64    `json:"processed_events"`
	ErrorCount      uint64    `json:"error_count"`
	LastActivity    time.Time `json:"last_activity"`
}
