package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/config"
	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
	"github.com/nashabanov/ueba-event-generator/internal/pipeline/coordinator"
	"github.com/nashabanov/ueba-event-generator/internal/pipeline/stages"
)

// Application основная структура приложения
type Application struct {
	config    *config.Config
	pipeline  coordinator.Pipeline
	genStage  *stages.EventGenerationStage // ✅ Ссылка на стадию генерации
	sendStage *stages.NetworkSendingStage  // ✅ Ссылка на стадию отправки
}

// NewApplication создает новое приложение
func NewApplication(cfg *config.Config) *Application {
	return &Application{
		config: cfg,
	}
}

// Run запускает приложение и управляет жизненным циклом
func (app *Application) Run() error {
	// Создаем pipeline на основе конфигурации
	if err := app.createPipeline(); err != nil {
		return fmt.Errorf("failed to create pipeline: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Поддержка duration из конфигурации
	if app.config.Generator.Duration > 0 {
		log.Printf("Application will run for: %v", app.config.Generator.Duration)
		ctx, cancel = context.WithTimeout(ctx, app.config.Generator.Duration)
		defer cancel()
	}

	app.setupSignalHandling(cancel)

	log.Printf("Starting pipeline with 2 stages...")

	// Запуск мониторинга
	go app.monitorExecution(ctx)

	go func() {
		if err := app.pipeline.Start(ctx); err != nil {
			log.Printf("Pipeline error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()

	log.Println("Stopping pipeline...")
	if err := app.pipeline.Stop(); err != nil {
		log.Printf("Error stopping pipeline: %v", err)
	}

	// Финальная статистика
	app.printFinalStats()

	log.Println("Application stopped successfully")
	return nil
}

// createPipeline создает pipeline на основе конфигурации
func (app *Application) createPipeline() error {
	app.pipeline = coordinator.NewPipeline(app.config.Pipeline.BufferSize)

	// Создаем стадию генерации
	genStage, err := app.createGenerationStage()
	if err != nil {
		return fmt.Errorf("failed to create generation stage: %w", err)
	}
	// Сохраняем ссылку на реальную стадию
	app.genStage = genStage.(*stages.EventGenerationStage)

	// Создаем стадию отправки
	sendStage, err := app.createSendingStage()
	if err != nil {
		return fmt.Errorf("failed to create sending stage: %w", err)
	}
	// Сохраняем ссылку на реальную стадию
	app.sendStage = sendStage.(*stages.NetworkSendingStage)

	// Создаем адаптеры для интеграции с coordinator
	genAdapter := stages.NewGenerationAdapter(genStage, app.config.Pipeline.BufferSize)
	sendAdapter := stages.NewSendingAdapter(sendStage, app.config.Pipeline.BufferSize)

	// Добавляем стадии в pipeline
	if err := app.pipeline.AddStage(genAdapter); err != nil {
		return fmt.Errorf("failed to add generation stage: %w", err)
	}

	if err := app.pipeline.AddStage(sendAdapter); err != nil {
		return fmt.Errorf("failed to add sending stage: %w", err)
	}

	log.Printf("Pipeline created: Generator=%s, Sender=%s",
		genStage.Name(), sendStage.Name())

	return nil
}

// createGenerationStage создает стадию генерации на основе конфигурации
func (app *Application) createGenerationStage() (stages.GenerationStage, error) {
	genStage := stages.NewEventGenerationStage(
		app.config.Generator.Name,
		app.config.Generator.EventsPerSecond,
	)

	eventTypes, err := app.parseEventTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to parse event types: %w", err)
	}

	if err := genStage.SetEventTypes(eventTypes); err != nil {
		return nil, fmt.Errorf("failed to set event types: %w", err)
	}

	log.Printf("Generation stage configured: %d events/sec, types=%v",
		app.config.Generator.EventsPerSecond,
		app.config.Generator.EventTypes)

	return genStage, nil
}

// createSendingStage создает стадию отправки на основе конфигурации
func (app *Application) createSendingStage() (stages.SendingStage, error) {
	sendStage := stages.NewNetworkSendingStage(app.config.Sender.Name)

	if err := sendStage.SetDestinations(app.config.Sender.Destinations); err != nil {
		return nil, fmt.Errorf("failed to set destinations: %w", err)
	}

	if err := sendStage.SetProtocol(app.config.Sender.Protocol); err != nil {
		return nil, fmt.Errorf("failed to set protocol: %w", err)
	}

	log.Printf("Sending stage configured: protocol=%s, destinations=%v",
		app.config.Sender.Protocol,
		app.config.Sender.Destinations)

	return sendStage, nil
}

// parseEventTypes конвертирует строки из конфига в EventType
func (app *Application) parseEventTypes() ([]event.EventType, error) {
	eventTypes := make([]event.EventType, len(app.config.Generator.EventTypes))

	for i, typeStr := range app.config.Generator.EventTypes {
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

// monitorExecution мониторит выполнение и выводит статистику
func (app *Application) monitorExecution(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ticker.C:
			app.printCurrentStats(startTime)
		case <-ctx.Done():
			return
		}
	}
}

// printCurrentStats выводит текущую статистику
func (app *Application) printCurrentStats(startTime time.Time) {
	duration := time.Since(startTime)

	var generated, sent, failed uint64
	if app.genStage != nil {
		generated = app.genStage.GetGeneratedCount()
	}
	if app.sendStage != nil {
		sent = app.sendStage.GetSentCount()
		failed = app.sendStage.GetFailedCount()
	}

	rate := float64(generated) / duration.Seconds()

	log.Printf("📊 Stats: Generated=%d, Sent=%d, Failed=%d, Rate=%.1f/sec, Runtime=%v",
		generated, sent, failed, rate, duration.Truncate(time.Second))
}

// printFinalStats выводит финальную статистику
func (app *Application) printFinalStats() {
	log.Printf("=== Final Statistics ===")

	if app.genStage != nil {
		log.Printf("🔄 Generation: Generated=%d, Errors=%d",
			app.genStage.GetGeneratedCount(), app.genStage.GetFailedCount())
	}

	if app.sendStage != nil {
		log.Printf("📤 Sending: Sent=%d, Failed=%d",
			app.sendStage.GetSentCount(), app.sendStage.GetFailedCount())
	}

	log.Printf("========================")
}

// setupSignalHandling настраивает обработку системных сигналов
func (app *Application) setupSignalHandling(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v, initiating graceful shutdown...", sig)
		cancel()
	}()
}
