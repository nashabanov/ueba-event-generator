package app

import (
	"context"
	"errors"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/config"
	"github.com/nashabanov/ueba-event-generator/internal/pipeline/coordinator"
	"github.com/stretchr/testify/mock"
)

// MockPipeline реализует мок для интерфейса coordinator.Pipeline
type MockPipeline struct {
	mock.Mock
}

func (m *MockPipeline) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockPipeline) Stop() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockPipeline) AddStage(stage coordinator.Stage) error {
	args := m.Called(stage)
	return args.Error(0)
}

func (m *MockPipeline) GetStatus() coordinator.PipelineStatus {
	args := m.Called()
	return args.Get(0).(coordinator.PipelineStatus)
}

// MockMonitor реализует мок мониторинга
type MockMonitor struct {
	mock.Mock
}

func (m *MockMonitor) Start(ctx context.Context) {
	m.Called(ctx)
}

func (m *MockMonitor) Stop() error {
	args := m.Called()
	return args.Error(0)
}

// MockLogger реализует мок логгера
type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) Debug(msg string, args ...any) {
	m.Called(append([]any{msg}, args...)...)
}

func (m *MockLogger) Info(msg string, args ...any) {
	m.Called(append([]any{msg}, args...)...)
}

func (m *MockLogger) Warn(msg string, args ...any) {
	m.Called(append([]any{msg}, args...)...)
}

func (m *MockLogger) Error(msg string, args ...any) {
	m.Called(append([]any{msg}, args...)...)
}

func newTestApplication(_ *testing.T) (*Application, *MockPipeline, *MockMonitor, *MockLogger) {
	cfg := config.DefaultConfig()
	pipeline := new(MockPipeline)
	monitor := new(MockMonitor)
	log := new(MockLogger)

	return NewApplication(cfg, pipeline, monitor, log), pipeline, monitor, log
}

func TestNewApplication(t *testing.T) {
	app, pipeline, monitor, logger := newTestApplication(t)

	if app == nil {
		t.Fatal("Expected non-nil application")
	}
	if app.pipeline != pipeline {
		t.Error("Pipeline not properly set")
	}
	if app.monitor != monitor {
		t.Error("Monitor not properly set")
	}
	if app.logger != logger {
		t.Error("Logger not properly set")
	}
}

func TestApplicationRun(t *testing.T) {
	tests := []struct {
		name          string
		duration      time.Duration
		setupMocks    func(*MockPipeline, *MockMonitor, *MockLogger)
		expectedError bool
	}{
		{
			name:     "Successful run with duration",
			duration: 100 * time.Millisecond,
			setupMocks: func(p *MockPipeline, m *MockMonitor, l *MockLogger) {
				p.On("Start", mock.Anything).Return(nil)
				p.On("Stop").Return(nil)
				m.On("Start", mock.Anything)
				m.On("Stop").Return(nil)

				l.On("Info", mock.Anything).Return().Maybe()
				l.On("Info", mock.Anything, mock.Anything).Return().Maybe()
				l.On("Error", mock.Anything).Return().Maybe()
				l.On("Error", mock.Anything, mock.Anything).Return().Maybe()
			},
		},
		{
			name:     "Pipeline start error",
			duration: 100 * time.Millisecond,
			setupMocks: func(p *MockPipeline, m *MockMonitor, l *MockLogger) {
				p.On("Start", mock.Anything).Return(errors.New("pipeline start error"))
				p.On("Stop").Return(nil)
				m.On("Start", mock.Anything)
				m.On("Stop").Return(nil)

				l.On("Info", mock.Anything).Return().Maybe()
				l.On("Info", mock.Anything, mock.Anything).Return().Maybe()
				l.On("Error", mock.Anything).Return().Maybe()
				l.On("Error", mock.Anything, mock.Anything).Return().Maybe()
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, pipeline, monitor, logger := newTestApplication(t)
			app.config.Generator.Duration = tt.duration

			tt.setupMocks(pipeline, monitor, logger)

			// Run the application in a goroutine
			done := make(chan error)
			go func() {
				done <- app.Run()
			}()

			// If duration is set, wait for it to complete
			// Otherwise, manually stop after a short delay
			if tt.duration > 0 {
				time.Sleep(tt.duration + 50*time.Millisecond)
			} else {
				time.Sleep(50 * time.Millisecond)
				app.Stop()
			}

			err := <-done

			if (err != nil) != tt.expectedError {
				t.Errorf("expected error: %v, got: %v", tt.expectedError, err)
			}

			pipeline.AssertExpectations(t)
			monitor.AssertExpectations(t)
			logger.AssertExpectations(t)
		})
	}
}

func TestSignalHandling(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Signal handling test not supported on Windows")
	}

	app, pipeline, monitor, logger := newTestApplication(t)

	pipeline.On("Start", mock.Anything).Return(nil)
	pipeline.On("Stop").Return(nil)
	monitor.On("Start", mock.Anything)
	monitor.On("Stop").Return(nil)
	logger.On("Info", mock.Anything, mock.Anything).Return().Maybe()

	done := make(chan error)
	go func() {
		done <- app.Run()
	}()

	time.Sleep(50 * time.Millisecond)

	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("Failed to find process: %v", err)
	}

	err = p.Signal(syscall.SIGTERM)
	if err != nil {
		t.Fatalf("Failed to send signal: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("Application did not stop within timeout")
	}

	pipeline.AssertExpectations(t)
	monitor.AssertExpectations(t)
	logger.AssertExpectations(t)
}

func TestApplicationStop(t *testing.T) {
	app, pipeline, monitor, logger := newTestApplication(t)

	pipeline.On("Start", mock.Anything).Return(nil)
	pipeline.On("Stop").Return(nil)
	monitor.On("Start", mock.Anything)
	monitor.On("Stop").Return(nil)
	logger.On("Info", mock.Anything, mock.Anything).Return()

	done := make(chan error)
	go func() {
		done <- app.Run()
	}()

	// Wait a bit for the app to start
	time.Sleep(50 * time.Millisecond)

	app.Stop()

	select {
	case <-done:
		// Application stopped as expected
	case <-time.After(time.Second):
		t.Error("Application did not stop within timeout")
	}

	pipeline.AssertExpectations(t)
	monitor.AssertExpectations(t)
	logger.AssertExpectations(t)
}
