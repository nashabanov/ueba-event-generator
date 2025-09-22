package stages

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
)

// GenerationStageAdapter адаптирует GenerationStage к общему Stage интерфейсу
type GenerationStageAdapter struct {
	generationStage GenerationStage
	bufferSize      int
}

// NewGenerationAdapter создает новый адаптер для генерации
func NewGenerationAdapter(stage GenerationStage, bufferSize int) *GenerationStageAdapter {
	return &GenerationStageAdapter{
		generationStage: stage,
		bufferSize:      bufferSize,
	}
}

func (a *GenerationStageAdapter) Name() string {
	return a.generationStage.Name()
}

// GenerationStageAdapter - ИСПРАВЛЕННАЯ ВЕРСИЯ
func (a *GenerationStageAdapter) Run(ctx context.Context, in <-chan event.Event, out chan<- event.Event) error {
	serializedChan := make(chan *SerializedData, a.bufferSize)

	errChan := make(chan error, 1)
	go func() {
		defer close(serializedChan)
		if err := a.generationStage.Run(ctx, serializedChan); err != nil {
			errChan <- fmt.Errorf("generation stage error: %w", err)
		}
	}()

	for {
		select {
		case serializedData, ok := <-serializedChan:
			if !ok {
				// Канал закрыт - завершаемся
				return nil
			}

			wrappedEvent := NewSerializedEvent(serializedData)

			select {
			case out <- wrappedEvent:
				// fmt.Printf("✅ GenerationAdapter: sent to pipeline\n")
			case <-ctx.Done():
				return ctx.Err()
			}

		case err := <-errChan:
			return err

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// SendingStageAdapter адаптирует SendingStage к общему Stage интерфейсу
type SendingStageAdapter struct {
	sendingStage SendingStage
	bufferSize   int
}

// NewSendingAdapter создает новый адаптер для отправки
func NewSendingAdapter(stage SendingStage, bufferSize int) *SendingStageAdapter {
	return &SendingStageAdapter{
		sendingStage: stage,
		bufferSize:   bufferSize,
	}
}

func (a *SendingStageAdapter) Name() string {
	return a.sendingStage.Name()
}

func (a *SendingStageAdapter) Run(ctx context.Context, in <-chan event.Event, out chan<- event.Event) error {
	serializedChan := make(chan *SerializedData, a.bufferSize)

	errChan := make(chan error, 1)
	go func() {
		if err := a.sendingStage.Run(ctx, serializedChan); err != nil {
			errChan <- fmt.Errorf("sending stage error: %w", err)
		}
	}()

	// Функция для правильного закрытия канала
	defer close(serializedChan)

	for {
		select {
		case incomingEvent, ok := <-in:
			if !ok {
				// Входной канал закрыт - завершаемся
				return nil
			}

			serializedData, err := a.extractSerializedData(incomingEvent)
			if err != nil {
				return fmt.Errorf("failed to extract serialized data: %w", err)
			}

			select {
			case serializedChan <- serializedData:
				// fmt.Printf("✅ SendingAdapter: sent to SendingStage\n")
			case <-ctx.Done():
				return ctx.Err()
			}

		case err := <-errChan:
			return err

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// extractSerializedData извлекает SerializedData из события
func (a *SendingStageAdapter) extractSerializedData(ev event.Event) (*SerializedData, error) {
	// Проверяем что это SerializedEvent
	serializedEvent, ok := ev.(*SerializedEvent)
	if !ok {
		return nil, fmt.Errorf("expected SerializedEvent, got %T", ev)
	}

	return serializedEvent.GetSerializedData(), nil
}

// SerializedEvent оборачивает SerializedData для совместимости с event.Event
type SerializedEvent struct {
	serializedData *SerializedData
}

// NewSerializedEvent создает обертку
func NewSerializedEvent(data *SerializedData) *SerializedEvent {
	return &SerializedEvent{serializedData: data}
}

// Реализуем интерфейс event.Event
func (se *SerializedEvent) Type() event.EventType {
	return se.serializedData.EventType
}

func (se *SerializedEvent) Timestamp() time.Time {
	return se.serializedData.Timestamp
}

func (se *SerializedEvent) Size() int {
	return se.serializedData.Size
}

func (se *SerializedEvent) Validate() error {
	return se.serializedData.Validate()
}

// GetID implements event.Event interface for SerializedEvent
func (se *SerializedEvent) GetID() string {
	return se.serializedData.EventID
}

func (se *SerializedEvent) ToJSON() ([]byte, error) {
	return se.serializedData.Data, nil // Уже сериализованные данные
}

func (se *SerializedEvent) GetSourceIP() netip.Addr {
	return netip.Addr{} // Не актуально для сериализованных данных
}

func (se *SerializedEvent) GetDestinationIP() netip.Addr {
	return netip.Addr{} // Не актуально для сериализованных данных
}

// GetSerializedData возвращает внутренние данные
func (se *SerializedEvent) GetSerializedData() *SerializedData {
	return se.serializedData
}
