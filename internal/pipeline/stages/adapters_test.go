package stages

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
	"github.com/stretchr/testify/require"
)

// --- Мок для GenerationStage ---
type mockGenerationStage struct {
	runErr error
	data   *SerializedData
}

// GetGeneratedCount implements GenerationStage.
func (m *mockGenerationStage) GetGeneratedCount() uint64 {
	panic("unimplemented")
}

// SetEventRate implements GenerationStage.
func (m *mockGenerationStage) SetEventRate(eventPerSecond int) error {
	panic("unimplemented")
}

// SetEventTypes implements GenerationStage.
func (m *mockGenerationStage) SetEventTypes(types []event.EventType) error {
	panic("unimplemented")
}

func (m *mockGenerationStage) GetFailedCount() uint64 {
	return 0
}

func (m *mockGenerationStage) Name() string { return "mockGenStage" }

func (m *mockGenerationStage) Run(ctx context.Context, out chan<- *SerializedData) error {
	if m.data != nil {
		out <- m.data
	}
	return m.runErr
}

// --- Мок для SendingStage ---
type mockSendingStage struct {
	runErr error
}

// GetSentCount implements SendingStage.
func (m *mockSendingStage) GetSentCount() uint64 {
	panic("unimplemented")
}

// SetDestinations implements SendingStage.
func (m *mockSendingStage) SetDestinations(destinations []string) error {
	panic("unimplemented")
}

// SetProtocol implements SendingStage.
func (m *mockSendingStage) SetProtocol(protocol string) error {
	panic("unimplemented")
}

func (m *mockSendingStage) GetFailedCount() uint64 {
	return 0
}

func (m *mockSendingStage) Name() string { return "mockSendStage" }

func (m *mockSendingStage) Run(ctx context.Context, in <-chan *SerializedData) error {
	for range in {
		// просто читаем все данные
	}
	return m.runErr
}

// --- Тесты GenerationStageAdapter ---

func TestGenerationStageAdapter_Run(t *testing.T) {
	data := &SerializedData{
		Data:      []byte(`{"foo":"bar"}`),
		EventID:   "evt_1",
		EventType: event.EventTypeNetflow,
		Size:      10,
		Timestamp: time.Now(),
	}

	mockGen := &mockGenerationStage{data: data}
	adapter := NewGenerationAdapter(mockGen, 1)

	out := make(chan event.Event, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := adapter.Run(ctx, nil, out)
	require.NoError(t, err)

	select {
	case ev := <-out:
		require.Equal(t, "evt_1", ev.GetID())
		require.Equal(t, event.EventTypeNetflow, ev.Type())
	default:
		t.Fatal("Expected event in output channel")
	}
}

func TestGenerationStageAdapter_Run_ContextCancel(t *testing.T) {
	mockGen := &mockGenerationStage{runErr: nil}
	adapter := NewGenerationAdapter(mockGen, 1)

	out := make(chan event.Event, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := adapter.Run(ctx, nil, out)
	require.ErrorIs(t, err, context.Canceled)
}

// --- Тесты SendingStageAdapter ---

func TestSendingStageAdapter_Run(t *testing.T) {
	mockSend := &mockSendingStage{}
	adapter := NewSendingAdapter(mockSend, 1)

	in := make(chan event.Event, 1)
	data := &SerializedData{
		Data:      []byte(`{"foo":"bar"}`),
		EventID:   "evt_2",
		EventType: event.EventTypeNetflow,
		Size:      5,
		Timestamp: time.Now(),
	}
	in <- NewSerializedEvent(data)
	close(in)

	out := make(chan event.Event, 1)
	ctx := context.Background()

	err := adapter.Run(ctx, in, out)
	require.NoError(t, err)
}

func TestSendingStageAdapter_Run_InvalidEvent(t *testing.T) {
	mockSend := &mockSendingStage{}
	adapter := NewSendingAdapter(mockSend, 1)

	in := make(chan event.Event, 1)
	in <- &mockInvalidEvent{}
	close(in)

	out := make(chan event.Event, 1)
	ctx := context.Background()

	err := adapter.Run(ctx, in, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected SerializedEvent")
}

// --- Вспомогательный мок для ошибки ---
type mockInvalidEvent struct{}

func (m *mockInvalidEvent) Type() event.EventType        { return 0 }
func (m *mockInvalidEvent) Timestamp() time.Time         { return time.Time{} }
func (m *mockInvalidEvent) Size() int                    { return 0 }
func (m *mockInvalidEvent) Validate() error              { return nil }
func (m *mockInvalidEvent) GetID() string                { return "" }
func (m *mockInvalidEvent) ToJSON() ([]byte, error)      { return nil, nil }
func (m *mockInvalidEvent) GetSourceIP() netip.Addr      { return netip.Addr{} }
func (m *mockInvalidEvent) GetDestinationIP() netip.Addr { return netip.Addr{} }
