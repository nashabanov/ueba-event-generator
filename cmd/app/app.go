package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/nashabanov/ueba-event-generator/internal/config"
	"github.com/nashabanov/ueba-event-generator/internal/logger"
	"github.com/nashabanov/ueba-event-generator/internal/monitoring"
	"github.com/nashabanov/ueba-event-generator/internal/pipeline/coordinator"
)

// Application основная структура приложения
type Application struct {
	config   *config.Config
	pipeline coordinator.Pipeline
	monitor  monitoring.Monitor
	logger   logger.Logger
}

// NewApplication создает новое приложение
func NewApplication(cfg *config.Config, p coordinator.Pipeline, m monitoring.Monitor, log logger.Logger) *Application {
	return &Application{
		config:   cfg,
		pipeline: p,
		monitor:  m,
		logger:   log,
	}
}

// Run запускает приложение и управляет жизненным циклом
func (app *Application) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if app.config.Generator.Duration > 0 {
		app.logger.Info("Application will run for: %v", app.config.Generator.Duration)
		ctx, cancel = context.WithTimeout(ctx, app.config.Generator.Duration)
		defer cancel()
	}

	app.setupSignalHandling(cancel)

	go app.monitor.Start(ctx)

	app.logger.Info("Starting pipeline...")

	go func() {
		if err := app.pipeline.Start(ctx); err != nil {
			app.logger.Error("Pipeline error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()

	app.logger.Info("Stopping pipeline...")
	if err := app.pipeline.Stop(); err != nil {
		app.logger.Error("Error stopping pipeline: %v", err)
	}

	app.logger.Info("Stopping monitor...")
	if err := app.monitor.Stop(); err != nil {
		app.logger.Error("Failed to stop monitor: %v", err)
	}

	app.logger.Info("Application stopped successfully")
	return nil
}

// setupSignalHandling настраивает обработку системных сигналов
func (app *Application) setupSignalHandling(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		app.logger.Info("Received signal: %v, initiating graceful shutdown...", sig)
		cancel()
	}()
}
