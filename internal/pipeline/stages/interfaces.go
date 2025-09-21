package stages

import (
	"context"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
)

// GenerationStage интерфейс для стадии генерации + сериализации
type GenerationStage interface {
	Name() string
	Run(ctx context.Context, out chan<- *SerializedData) error

	// Конфигурация генерации
	SetEventRate(eventPerSecond int) error
	SetEventTypes(types []event.EventType) error

	// Метрики
	GetGeneratedCount() uint64
	GetFailedCount() uint64
}

// SendingStage интерфейс для стадии отправки
type SendingStage interface {
	Name() string
	Run(ctx context.Context, in <-chan *SerializedData) error

	// Конфигурация отправки
	SetDestinations(destinations []string) error
	SetProtocol(protocol string) error

	// Метрики
	GetSentCount() uint64
	GetFailedCount() uint64
}

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
