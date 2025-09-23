package factory

import (
	"fmt"
	"strings"

	"github.com/nashabanov/ueba-event-generator/internal/config"
	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
	"github.com/nashabanov/ueba-event-generator/internal/pipeline/coordinator"
	"github.com/nashabanov/ueba-event-generator/internal/pipeline/stages"
)

// PipelineFactory отвечает за создание и конфигурацию pipeline
type PipelineFactory struct {
	cfg *config.Config
}

// NewPipelineFactory конструктор фабрики
func NewPipelineFactory(cfg *config.Config) *PipelineFactory {
	return &PipelineFactory{cfg: cfg}
}

// CreatePipeline создает pipeline с настроенными стадиями
func (f *PipelineFactory) CreatePipeline() (coordinator.Pipeline, error) {
	pipeline := coordinator.NewPipeline(f.cfg.Pipeline.BufferSize)

	genStage, err := f.createGenerationStage()
	if err != nil {
		return nil, fmt.Errorf("failed to create generation stage: %w", err)
	}

	sendStage, err := f.createSendingStage()
	if err != nil {
		return nil, fmt.Errorf("failed to create sending stage: %w", err)
	}

	genAdapter := stages.NewGenerationAdapter(genStage, f.cfg.Pipeline.BufferSize)
	sendAdapter := stages.NewSendingAdapter(sendStage, f.cfg.Pipeline.BufferSize)

	if err := pipeline.AddStage(genAdapter); err != nil {
		return nil, fmt.Errorf("failed to add generation stage: %w", err)
	}

	if err := pipeline.AddStage(sendAdapter); err != nil {
		return nil, fmt.Errorf("failed to add sending stage: %w", err)
	}

	return pipeline, nil
}

func (f *PipelineFactory) createGenerationStage() (stages.GenerationStage, error) {
	genStage := stages.NewEventGenerationStage(
		f.cfg.Generator.Name,
		f.cfg.Generator.EventsPerSecond,
	)

	eventTypes, err := f.ParseEventTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to parse event types: %w", err)
	}

	if err := genStage.SetEventTypes(eventTypes); err != nil {
		return nil, fmt.Errorf("failed to set event types: %w", err)
	}

	return genStage, nil
}

func (f *PipelineFactory) createSendingStage() (stages.SendingStage, error) {
	sendStage := stages.NewNetworkSendingStage(f.cfg.Sender.Name)

	if err := sendStage.SetDestinations(f.cfg.Sender.Destinations); err != nil {
		return nil, fmt.Errorf("failed to set destinations: %w", err)
	}

	if err := sendStage.SetProtocol(f.cfg.Sender.Protocol); err != nil {
		return nil, fmt.Errorf("failed to set protocol: %w", err)
	}

	return sendStage, nil
}

func (f *PipelineFactory) ParseEventTypes() ([]event.EventType, error) {
	eventTypes := make([]event.EventType, len(f.cfg.Generator.EventTypes))

	for i, typeStr := range f.cfg.Generator.EventTypes {
		switch strings.ToLower(typeStr) {
		case "netflow":
			eventTypes[i] = event.EventTypeNetflow
		case "syslog":
			eventTypes[i] = event.EventTypeSyslog
		default:
			return nil, fmt.Errorf("unsupported event type: %s", typeStr)
		}
	}

	return eventTypes, nil
}
