package main

import (
	"strings"
	"testing"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/config"
)

func TestNewApplication(t *testing.T) {
	cfg := config.DefaultConfig()
	app := NewApplication(cfg)

	if app == nil {
		t.Fatal("NewApplication returned nil")
	}

	if app.config != cfg {
		t.Error("Application config not set correctly")
	}

	if app.genStage != nil {
		t.Error("genStage should be nil before createPipeline")
	}

	if app.sendStage != nil {
		t.Error("sendStage should be nil before createPipeline")
	}
}

func TestCreatePipeline(t *testing.T) {
	cfg := config.DefaultConfig()
	app := NewApplication(cfg)

	err := app.createPipeline()
	if err != nil {
		t.Fatalf("createPipeline failed: %v", err)
	}

	if app.pipeline == nil {
		t.Error("Pipeline not created")
	}

	if app.genStage == nil {
		t.Error("Generation stage not created")
	}

	if app.sendStage == nil {
		t.Error("Sending stage not created")
	}

	// Проверяем имена стадий
	if app.genStage.Name() != cfg.Generator.Name {
		t.Errorf("Expected generation stage name '%s', got '%s'",
			cfg.Generator.Name, app.genStage.Name())
	}

	if app.sendStage.Name() != cfg.Sender.Name {
		t.Errorf("Expected sending stage name '%s', got '%s'",
			cfg.Sender.Name, app.sendStage.Name())
	}
}

func TestCreatePipelineWithInvalidEventTypes(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Generator.EventTypes = []string{"invalid-type"}

	app := NewApplication(cfg)

	err := app.createPipeline()
	if err == nil {
		t.Error("Expected error for invalid event type, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported event type") {
		t.Errorf("Expected 'unsupported event type' error, got: %v", err)
	}
}

func TestParseEventTypes(t *testing.T) {
	cfg := config.DefaultConfig()
	app := NewApplication(cfg)

	tests := []struct {
		name        string
		eventTypes  []string
		expectError bool
		expectedLen int
	}{
		{
			name:        "Valid netflow",
			eventTypes:  []string{"netflow"},
			expectError: false,
			expectedLen: 1,
		},
		{
			name:        "Valid syslog",
			eventTypes:  []string{"syslog"},
			expectError: false,
			expectedLen: 1,
		},
		{
			name:        "Both types",
			eventTypes:  []string{"netflow", "syslog"},
			expectError: false,
			expectedLen: 2,
		},
		{
			name:        "Case insensitive",
			eventTypes:  []string{"NETFLOW", "SysLog"},
			expectError: false,
			expectedLen: 2,
		},
		{
			name:        "Invalid type",
			eventTypes:  []string{"invalid"},
			expectError: true,
			expectedLen: 0,
		},
		{
			name:        "Mixed valid and invalid",
			eventTypes:  []string{"netflow", "invalid"},
			expectError: true,
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app.config.Generator.EventTypes = tt.eventTypes

			eventTypes, err := app.parseEventTypes()

			if tt.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got: %v", err)
				}

				if len(eventTypes) != tt.expectedLen {
					t.Errorf("Expected %d event types, got %d", tt.expectedLen, len(eventTypes))
				}
			}
		})
	}
}

func TestApplicationRunWithTimeout(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Generator.Duration = 100 * time.Millisecond // Очень короткий timeout для теста
	cfg.Generator.EventsPerSecond = 1               // Низкий rate для стабильности

	app := NewApplication(cfg)

	start := time.Now()
	err := app.Run()
	duration := time.Since(start)

	if err != nil {
		t.Errorf("Run failed: %v", err)
	}

	// Проверяем что приложение завершилось примерно через указанное время
	expectedDuration := cfg.Generator.Duration
	tolerance := 200 * time.Millisecond

	if duration < expectedDuration || duration > expectedDuration+tolerance {
		t.Errorf("Expected duration ~%v, got %v", expectedDuration, duration)
	}

	// Проверяем что стадии были созданы
	if app.genStage == nil {
		t.Error("Generation stage was not created")
	}

	if app.sendStage == nil {
		t.Error("Sending stage was not created")
	}
}

// Функция helper для интеграционного теста
func TestApplicationRunShortDuration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := config.DefaultConfig()
	cfg.Generator.Duration = 2 * time.Second
	cfg.Generator.EventsPerSecond = 5
	cfg.Sender.Destinations = []string{"127.0.0.1:9999"} // Несуществующий порт

	app := NewApplication(cfg)

	// Запускаем в горутине чтобы не блокировать тест
	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Ждем завершения с таймаутом
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Application did not complete within expected time")
	}
}
