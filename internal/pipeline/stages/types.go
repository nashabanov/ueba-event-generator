package stages

import (
	"fmt"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
)

// SerializedData представляет готовые данные для отправки
type SerializedData struct {
	Data      []byte          `json:"data"`
	EventType event.EventType `json:"event_type"`
	Size      int             `json:"size"`
	Timestamp time.Time       `json:"timestamp"`
	EventID   string          `json:"event_id"`

	Destination string `json:"destination"` // Куда отправлять (IP:port)
	Protocol    string `json:"protocol"`    // UDP/TCP
}

// NewSerializedData создает новый экземпляр SerializedData
func NewSerializedData(data []byte, eventType event.EventType, eventID string) *SerializedData {
	return &SerializedData{
		Data:      data,
		EventType: eventType,
		Size:      len(data),
		Timestamp: time.Now().UTC(),
		EventID:   eventID,
	}
}

// Validate проверяет корректность данных
func (sd *SerializedData) Validate() error {
	if sd.EventID == "" {
		return fmt.Errorf("event ID cannot be empty")
	}

	if sd.Size != len(sd.Data) {
		return fmt.Errorf("size mismatch: expected %d, got %d", len(sd.Data), sd.Size)
	}

	if len(sd.Data) == 0 {
		return fmt.Errorf("serialized data cannot be empty")
	}

	return nil
}

// String возвращает строковое представление SerializedData
func (sd *SerializedData) String() string {
	return fmt.Sprintf("SerializedData{Type: %s, Size: %d, ID: %s}",
		sd.EventType, sd.Size, sd.EventID)
}
