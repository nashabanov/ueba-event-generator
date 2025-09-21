package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
)

// MockStage - заглушка стадии для тестирования
type MockStage struct {
	name     string
	runCount int
	runError error
}

func NewMockStage(name string) *MockStage {
	return &MockStage{
		name: name,
	}
}

func (m *MockStage) Name() string {
	return m.name
}

func (m *MockStage) Run(ctx context.Context, in <-chan event.Event, out chan<- event.Event) error {
	m.runCount++

	// Простая логика: читаем из in, передаем в out
	for {
		select {
		case event, ok := <-in:
			if !ok {
				// Канал закрыт - завершаемся
				return m.runError
			}

			// Передаем событие дальше (если есть out канал)
			if out != nil {
				select {
				case out <- event:
					// Отправили успешно
				case <-ctx.Done():
					return ctx.Err()
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// SetError устанавливает ошибку для тестирования
func (m *MockStage) SetError(err error) {
	m.runError = err
}

// GetRunCount возвращает количество запусков
func (m *MockStage) GetRunCount() int {
	return m.runCount
}

func TestPipeline_Creation(t *testing.T) {
	pipeline := NewPipeline(100)

	if pipeline == nil {
		t.Fatal("Pipeline should not be nil")
	}

	status := pipeline.GetStatus()
	if status != Stopped {
		t.Errorf("Initial status should be Stopped, got %v", status)
	}
}

func TestPipeline_AddStage(t *testing.T) {
	pipeline := NewPipeline(100)
	stage := NewMockStage("test-stage")

	err := pipeline.AddStage(stage)
	if err != nil {
		t.Errorf("AddStage should not return error: %v", err)
	}

	// Проверяем что нельзя добавить стадию к запущенному pipeline
	ctx := context.Background()
	pipeline.Start(ctx)

	stage2 := NewMockStage("test-stage-2")
	err = pipeline.AddStage(stage2)
	if err == nil {
		t.Error("Should not be able to add stage to running pipeline")
	}

	pipeline.Stop()
}

func TestPipeline_StartStop(t *testing.T) {
	pipeline := NewPipeline(100)

	// Проверяем что нельзя остановить неработающий pipeline
	err := pipeline.Stop()
	if err == nil {
		t.Error("Should not be able to stop non-running pipeline")
	}

	// Запускаем
	ctx := context.Background()
	err = pipeline.Start(ctx)
	if err != nil {
		t.Errorf("Start should not return error: %v", err)
	}

	status := pipeline.GetStatus()
	if status != Running {
		t.Errorf("Status after start should be Running, got %v", status)
	}

	// Проверяем что нельзя запустить дважды
	err = pipeline.Start(ctx)
	if err == nil {
		t.Error("Should not be able to start already running pipeline")
	}

	// Останавливаем
	err = pipeline.Stop()
	if err != nil {
		t.Errorf("Stop should not return error: %v", err)
	}

	status = pipeline.GetStatus()
	if status != Stopped {
		t.Errorf("Status after stop should be Stopped, got %v", status)
	}
}

func TestPipeline_EventFlow(t *testing.T) {
	pipeline := NewPipeline(10)

	// Создаем две стадии
	stage1 := NewMockStage("generator")
	stage2 := NewMockStage("sender")

	pipeline.AddStage(stage1)
	pipeline.AddStage(stage2)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Запускаем pipeline
	err := pipeline.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// Получаем доступ к внутренним каналам (для тестирования)
	impl := pipeline.(*pipelineImpl)

	// Создаем тестовое событие
	event := event.NewNetflowEvent()

	// Отправляем событие в pipeline
	go func() {
		impl.inputChan <- event
	}()

	// Читаем результат
	select {
	case result := <-impl.outputChan:
		if result == nil {
			t.Error("Received nil event")
		}
		t.Logf("Successfully received event: %v", result)

	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for event")
	}

	// Останавливаем pipeline
	err = pipeline.Stop()
	if err != nil {
		t.Errorf("Failed to stop pipeline: %v", err)
	}

	// Проверяем что обе стадии запускались
	if stage1.GetRunCount() == 0 {
		t.Error("Stage1 was not executed")
	}

	if stage2.GetRunCount() == 0 {
		t.Error("Stage2 was not executed")
	}
}

func BenchmarkPipeline_Throughput(b *testing.B) {
	pipeline := NewPipeline(1000)

	stage := NewMockStage("processor")
	pipeline.AddStage(stage)

	ctx := context.Background()
	pipeline.Start(ctx)

	impl := pipeline.(*pipelineImpl)

	go func() {
		for range impl.outputChan {
			// Просто читаем и отбрасываем события
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		event := event.NewNetflowEvent()
		impl.inputChan <- event
	}

	pipeline.Stop()
}
