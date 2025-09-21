package stages

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
)

// EventGenerationStage реализует GenerationStage
type EventGenerationStage struct {
	name            string
	eventTypes      []event.EventType
	eventsPerSecond int

	// Метрики (atomic для thread-safety)
	generatedCount uint64
	errorCount     uint64
}

// NewEventGenerationStage создает новую стадию генерации
func NewEventGenerationStage(name string, eventsPerSecond int) *EventGenerationStage {
	return &EventGenerationStage{
		name:            name,
		eventsPerSecond: eventsPerSecond,
		eventTypes:      []event.EventType{event.EventTypeNetflow}, // Проверить что константа определена
	}
}

func (g *EventGenerationStage) Name() string {
	return g.name
}

func (g *EventGenerationStage) Run(ctx context.Context, out chan<- *SerializedData) error {
	interval := time.Second / time.Duration(g.eventsPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fmt.Printf("Generation stage '%s' started: %d events/sec\n", g.name, g.eventsPerSecond)

	for {
		select {
		case <-ticker.C:
			if err := g.generateAndSendEvent(ctx, out); err != nil {
				atomic.AddUint64(&g.errorCount, 1)
				fmt.Printf("Generation error: %v\n", err)
				continue
			}
			atomic.AddUint64(&g.generatedCount, 1)

		case <-ctx.Done():
			fmt.Printf("Generation stage '%s' stopped. Generated: %d, Errors: %d\n",
				g.name, g.GetGeneratedCount(), g.GetFailedCount())
			return ctx.Err()
		}
	}
}

// generateAndSendEvent используем существующие функции из пакета event
func (g *EventGenerationStage) generateAndSendEvent(ctx context.Context, out chan<- *SerializedData) error {
	eventType := g.eventTypes[0]

	// Используем существующие функции создания событий
	var evt event.Event
	switch eventType {
	case event.EventTypeNetflow:
		evt = event.NewNetflowEvent()
	case event.EventTypeSyslog:
		evt = event.NewSyslogEvent()
	default:
		return fmt.Errorf("unsupported event type: %v", eventType)
	}

	// Сериализуем в JSON
	jsonData, err := evt.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	// Создаем SerializedData
	serializedData := NewSerializedData(jsonData, evt.Type(), evt.GetID())

	// Отправляем в выходной канал
	select {
	case out <- serializedData:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Остальные методы без изменений...
func (g *EventGenerationStage) SetEventRate(eventsPerSecond int) error {
	if eventsPerSecond <= 0 {
		return fmt.Errorf("events per second must be positive, got: %d", eventsPerSecond)
	}
	g.eventsPerSecond = eventsPerSecond
	return nil
}

func (g *EventGenerationStage) SetEventTypes(types []event.EventType) error {
	if len(types) == 0 {
		return fmt.Errorf("event types cannot be empty")
	}
	g.eventTypes = types
	return nil
}

func (g *EventGenerationStage) GetGeneratedCount() uint64 {
	return atomic.LoadUint64(&g.generatedCount)
}

func (g *EventGenerationStage) GetFailedCount() uint64 {
	return atomic.LoadUint64(&g.errorCount)
}
