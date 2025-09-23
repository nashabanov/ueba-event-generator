package app

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nashabanov/ueba-event-generator/internal/config"
	"github.com/nashabanov/ueba-event-generator/internal/monitoring"
	"github.com/nashabanov/ueba-event-generator/internal/pipeline/coordinator"
)

// Application основная структура приложения
type Application struct {
	config   *config.Config
	pipeline coordinator.Pipeline
	monitor  monitoring.Monitor
}

// NewApplication создает новое приложение
func NewApplication(cfg *config.Config, p coordinator.Pipeline, m monitoring.Monitor) *Application {
	return &Application{
		config:   cfg,
		pipeline: p,
		monitor:  m,
	}
}

// Run запускает приложение и управляет жизненным циклом
func (app *Application) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if app.config.Generator.Duration > 0 {
		log.Printf("Application will run for: %v", app.config.Generator.Duration)
		ctx, cancel = context.WithTimeout(ctx, app.config.Generator.Duration)
		defer cancel()
	}

	app.setupSignalHandling(cancel)

	go app.monitor.Start(ctx)

	log.Printf("Starting pipeline...")

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

	log.Println("Stopping monitor...")
	if err := app.monitor.Stop(); err != nil {
		log.Printf("Failed to stop monitor: %v", err)
	}

	log.Println("Application stopped successfully")
	return nil
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
