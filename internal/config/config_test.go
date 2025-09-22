package config

import (
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Проверяем основные значения по умолчанию
	if cfg.Generator.Name != "default-generator" {
		t.Errorf("Expected generator name 'default-generator', got '%s'", cfg.Generator.Name)
	}

	if cfg.Generator.EventsPerSecond != 10 {
		t.Errorf("Expected events per second 10, got %d", cfg.Generator.EventsPerSecond)
	}

	if len(cfg.Generator.EventTypes) != 1 || cfg.Generator.EventTypes[0] != "netflow" {
		t.Errorf("Expected event types [netflow], got %v", cfg.Generator.EventTypes)
	}

	if cfg.Sender.Protocol != "udp" {
		t.Errorf("Expected protocol 'udp', got '%s'", cfg.Sender.Protocol)
	}

	if len(cfg.Sender.Destinations) != 1 || cfg.Sender.Destinations[0] != "127.0.0.1:514" {
		t.Errorf("Expected destinations [127.0.0.1:514], got %v", cfg.Sender.Destinations)
	}

	if cfg.Pipeline.BufferSize != 100 {
		t.Errorf("Expected buffer size 100, got %d", cfg.Pipeline.BufferSize)
	}

	if cfg.Logging.Level != "info" {
		t.Errorf("Expected log level 'info', got '%s'", cfg.Logging.Level)
	}
}

func TestDefaultNetflowConfig(t *testing.T) {
	cfg := DefaultNetflowConfig()

	if cfg.Version != 9 {
		t.Errorf("Expected netflow version 9, got %d", cfg.Version)
	}

	if len(cfg.SourceIPs) == 0 {
		t.Error("Expected source IPs to be non-empty")
	}

	if len(cfg.Protocols) == 0 {
		t.Error("Expected protocols to be non-empty")
	}

	// Проверяем что есть основные протоколы
	hasUDP := false
	hasTCP := false
	for _, proto := range cfg.Protocols {
		if proto == "UDP" {
			hasUDP = true
		}
		if proto == "TCP" {
			hasTCP = true
		}
	}

	if !hasUDP || !hasTCP {
		t.Errorf("Expected UDP and TCP protocols, got %v", cfg.Protocols)
	}
}

func TestDefaultSyslogConfig(t *testing.T) {
	cfg := DefaultSyslogConfig()

	if cfg.Facility != 16 {
		t.Errorf("Expected facility 16, got %d", cfg.Facility)
	}

	if cfg.Severity != 6 {
		t.Errorf("Expected severity 6, got %d", cfg.Severity)
	}

	if len(cfg.Hostnames) == 0 {
		t.Error("Expected hostnames to be non-empty")
	}

	if len(cfg.Programs) == 0 {
		t.Error("Expected programs to be non-empty")
	}
}
