package logger

import "log"

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type stdLogger struct{}

func NewStdLogger() *stdLogger {
	return &stdLogger{}
}

func (l *stdLogger) Debug(msg string, args ...any) {
	log.Printf("[DEBUG] "+msg, args...)
}

func (l *stdLogger) Info(msg string, args ...any) {
	log.Printf("[INFO] "+msg, args...)
}

func (l *stdLogger) Warn(msg string, args ...any) {
	log.Printf("[WARNING] "+msg, args...)
}

func (l *stdLogger) Error(msg string, args ...any) {
	log.Printf("[ERROR] "+msg, args...)
}
