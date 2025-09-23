package stages

import (
	"fmt"
	"testing"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
	"github.com/stretchr/testify/require"
)

func TestNewSerializedData(t *testing.T) {
	data := []byte("test payload")
	eventID := "evt_12345"
	eventType := event.EventTypeNetflow

	sd := NewSerializedData(data, eventType, eventID)

	require.Equal(t, data, sd.Data)
	require.Equal(t, eventType, sd.EventType)
	require.Equal(t, eventID, sd.EventID)
	require.Equal(t, len(data), sd.Size)
	require.WithinDuration(t, time.Now().UTC(), sd.Timestamp, time.Second)
}

func TestSerializedData_Validate(t *testing.T) {
	data := []byte("payload")
	eventID := "evt_123"

	sd := NewSerializedData(data, event.EventTypeSyslog, eventID)
	require.NoError(t, sd.Validate())

	// Пустой EventID
	sdEmptyID := NewSerializedData(data, event.EventTypeSyslog, "")
	err := sdEmptyID.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "event ID cannot be empty")

	// Несоответствие Size
	sdWrongSize := NewSerializedData(data, event.EventTypeSyslog, eventID)
	sdWrongSize.Size = sdWrongSize.Size + 1
	err = sdWrongSize.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "size mismatch")

	// Пустые данные
	sdEmptyData := NewSerializedData([]byte{}, event.EventTypeSyslog, eventID)
	err = sdEmptyData.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "serialized data cannot be empty")
}

func TestSerializedData_String(t *testing.T) {
	data := []byte("abc")
	eventID := "evt_1"
	eventType := event.EventTypeNetflow

	sd := NewSerializedData(data, eventType, eventID)
	expected := fmt.Sprintf("SerializedData{Type: %s, Size: %d, ID: %s}", eventType, len(data), eventID)
	require.Equal(t, expected, sd.String())
}
