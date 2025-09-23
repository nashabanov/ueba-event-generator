package stages

import (
	"context"
	"testing"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
	"github.com/stretchr/testify/require"
)

func TestNewEventGenerationStage(t *testing.T) {
	stage := NewEventGenerationStage("gen_stage", 1000)
	require.Equal(t, "gen_stage", stage.Name())
	require.Equal(t, 1000, stage.eventsPerSecond)
	require.NotNil(t, stage.workerPool)
	require.NotNil(t, stage.eventTypes)
	require.Contains(t, stage.eventTypes, event.EventTypeNetflow)
}

func TestSetEventRate(t *testing.T) {
	stage := NewEventGenerationStage("gen_stage", 10)
	require.NoError(t, stage.SetEventRate(500))
	require.Equal(t, 500, stage.eventsPerSecond)

	err := stage.SetEventRate(0)
	require.Error(t, err)
}

func TestSetEventTypes(t *testing.T) {
	stage := NewEventGenerationStage("gen_stage", 10)
	require.NoError(t, stage.SetEventTypes([]event.EventType{event.EventTypeSyslog}))
	require.Equal(t, []event.EventType{event.EventTypeSyslog}, stage.eventTypes)

	err := stage.SetEventTypes([]event.EventType{})
	require.Error(t, err)
}

func TestGenerateAndSendEvent(t *testing.T) {
	stage := NewEventGenerationStage("gen_stage", 10)
	out := make(chan *SerializedData, 1)

	err := stage.generateAndSendEvent(context.Background(), out)
	require.NoError(t, err)

	select {
	case data := <-out:
		require.NotNil(t, data)
		require.NotEmpty(t, data.Data)
		require.NotEmpty(t, data.EventID)
		require.Equal(t, event.EventTypeNetflow, data.EventType)
	default:
		t.Fatal("Expected event in channel")
	}
}

func TestEventGenerationRun_ContextCancel(t *testing.T) {
	stage := NewEventGenerationStage("gen_stage", 100)
	out := make(chan *SerializedData, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := stage.Run(ctx, out)
	require.ErrorIs(t, err, context.Canceled)
}

func TestEventGenerationJob_Execute(t *testing.T) {
	stage := NewEventGenerationStage("gen_stage", 10)
	out := make(chan *SerializedData, 1)

	job := &EventGenerationJob{
		stage: stage,
		out:   out,
	}

	err := job.Execute()
	require.NoError(t, err)

	select {
	case data := <-out:
		require.NotNil(t, data)
	default:
		t.Fatal("Expected event to be written to channel")
	}
}

func TestGetGeneratedAndFailedCount(t *testing.T) {
	stage := NewEventGenerationStage("gen_stage", 10)

	// Вызываем generateAndSendEvent для увеличения счетчика
	out := make(chan *SerializedData, 1)
	stage.generateAndSendEvent(context.Background(), out)

	gen := stage.GetGeneratedCount()
	fail := stage.GetFailedCount()

	require.GreaterOrEqual(t, gen, uint64(1))
	require.Equal(t, uint64(0), fail)
}

func TestRun_ProcessData_Error_GenerationStage(t *testing.T) {
	stage := NewEventGenerationStage("gen_stage", 10)
	out := make(chan *SerializedData, 1)

	// Подставим невалидный event type для генерации ошибки
	stage.eventTypes = []event.EventType{9999} // unsupported

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := stage.generateAndSendEvent(ctx, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported event type")
}
