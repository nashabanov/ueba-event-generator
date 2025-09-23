package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/config"
	"github.com/nashabanov/ueba-event-generator/internal/monitoring"
	"github.com/nashabanov/ueba-event-generator/internal/pipeline/coordinator"
	"github.com/nashabanov/ueba-event-generator/internal/pipeline/factory"
)

// Application основная структура приложения
type Application struct {
	config   *config.Config
	pipeline coordinator.Pipeline
	monitor  *monitoring.Monitor
}

// NewApplication создает новое приложение
func NewApplication(cfg *config.Config) (*Application, error) {
	factory := factory.NewPipelineFactory(cfg)
	pipeline, err := factory.CreatePipeline()
	if err != nil {
		return nil, err
	}

	monitor := monitoring.NewMonitor(10 * time.Second)

	return &Application{
		config:   cfg,
		pipeline: pipeline,
		monitor:  monitor,
	}, nil
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

	log.Printf("Starting pipeline with 2 stages...")

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
