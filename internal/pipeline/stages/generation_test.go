package stages

import (
	"context"
	"testing"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
)

func TestEventGenerationStage_Creation(t *testing.T) {
	stage := NewEventGenerationStage("test-generator", 100)

	if stage.Name() != "test-generator" {
		t.Errorf("Expected name 'test-generator', got '%s'", stage.Name())
	}

	if stage.eventsPerSecond != 100 {
		t.Errorf("Expected 100 events/sec, got %d", stage.eventsPerSecond)
	}

	if stage.GetGeneratedCount() != 0 {
		t.Errorf("Initial generated count should be 0, got %d", stage.GetGeneratedCount())
	}
}

func TestEventGenerationStage_SetEventRate(t *testing.T) {
	stage := NewEventGenerationStage("test", 10)

	// Валидная скорость
	err := stage.SetEventRate(500)
	if err != nil {
		t.Errorf("SetEventRate(500) should not error: %v", err)
	}

	if stage.eventsPerSecond != 500 {
		t.Errorf("Expected 500 events/sec after update, got %d", stage.eventsPerSecond)
	}

	// Невалидная скорость
	err = stage.SetEventRate(0)
	if err == nil {
		t.Error("SetEventRate(0) should return error")
	}

	err = stage.SetEventRate(-10)
	if err == nil {
		t.Error("SetEventRate(-10) should return error")
	}
}

func TestEventGenerationStage_SetEventTypes(t *testing.T) {
	stage := NewEventGenerationStage("test", 10)

	// Валидные типы событий
	types := []event.EventType{event.EventTypeNetflow, event.EventTypeSyslog}
	err := stage.SetEventTypes(types)
	if err != nil {
		t.Errorf("SetEventTypes should not error: %v", err)
	}

	// Пустой список
	err = stage.SetEventTypes([]event.EventType{})
	if err == nil {
		t.Error("SetEventTypes with empty slice should return error")
	}
}

func TestEventGenerationStage_GenerateEvents(t *testing.T) {
	stage := NewEventGenerationStage("test-gen", 10) // 10 events/sec

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Канал для сбора событий
	outputChan := make(chan *SerializedData, 10)

	// Запускаем генерацию в горутине
	errChan := make(chan error, 1)
	go func() {
		errChan <- stage.Run(ctx, outputChan)
	}()

	// Собираем события
	var events []*SerializedData

collectLoop:
	for {
		select {
		case data := <-outputChan:
			events = append(events, data)

		case <-time.After(300 * time.Millisecond):
			// Timeout - завершаем сбор
			break collectLoop

		case err := <-errChan:
			if err != context.DeadlineExceeded {
				t.Errorf("Unexpected error from Run(): %v", err)
			}
			break collectLoop
		}
	}

	// Проверяем результаты
	if len(events) == 0 {
		t.Error("No events were generated")
	}

	// За 200ms при 10 events/sec должно быть ~2 события (с погрешностью)
	if len(events) < 1 || len(events) > 4 {
		t.Errorf("Expected 1-4 events in 200ms, got %d", len(events))
	}

	// Проверяем первое событие
	if len(events) > 0 {
		firstEvent := events[0]

		if len(firstEvent.Data) == 0 {
			t.Error("Generated event data is empty")
		}

		if firstEvent.EventID == "" {
			t.Error("Generated event ID is empty")
		}

		if firstEvent.Size != len(firstEvent.Data) {
			t.Errorf("Event size mismatch: expected %d, got %d",
				len(firstEvent.Data), firstEvent.Size)
		}
	}

	// Проверяем что метрики обновились
	if stage.GetGeneratedCount() == 0 {
		t.Error("Generated count should be > 0")
	}
}

func TestEventGenerationStage_ContextCancellation(t *testing.T) {
	stage := NewEventGenerationStage("test", 1000) // Высокая частота

	ctx, cancel := context.WithCancel(context.Background())
	outputChan := make(chan *SerializedData, 100)

	// Запускаем генерацию
	errChan := make(chan error, 1)
	go func() {
		errChan <- stage.Run(ctx, outputChan)
	}()

	// Ждем немного, затем отменяем
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Проверяем что быстро завершается
	select {
	case err := <-errChan:
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Stage did not stop within 100ms after context cancellation")
	}
}

func BenchmarkEventGeneration(b *testing.B) {
	stage := NewEventGenerationStage("bench", 1000)
	ctx := context.Background()
	outputChan := make(chan *SerializedData, 1000)

	go func() {
		for range outputChan {
			// Просто читаем и выбрасываем события
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stage.generateAndSendEvent(ctx, outputChan)
	}
}
