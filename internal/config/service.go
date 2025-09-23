package config

import (
	"fmt"
)

// ConfigService для работы с конфигурацией
type ConfigService interface {
	Load() error
	GetConfig() *Config
}

// configService реализация ConfigService
type configService struct {
	config   *Config
	loader   *Loader
	filePath string
	flags    *Flags
}

// NewConfigService создает новый экземпляр ConfigService
func NewConfigService(filePath string, flags *Flags) ConfigService {
	return &configService{
		config:   DefaultConfig(),
		loader:   NewLoader(),
		filePath: filePath,
		flags:    flags,
	}
}

// Load загружает конфигурацию из файла, окружения, флагов и валидирует её
func (cs *configService) Load() error {
	if err := cs.loader.LoadFromFile(cs.filePath); err != nil {
		return fmt.Errorf("failed to load config from file %s: %w", cs.filePath, err)
	}

	if err := cs.loader.LoadFromEnv(); err != nil {
		return fmt.Errorf("failed to load config from env: %w", err)
	}

	cs.loader.ApplyFlags(cs.flags)

	cs.config = cs.loader.GetConfig()

	if err := cs.config.Validate(); err != nil {
		return fmt.Errorf("config validate failed: %w", err)
	}

	return nil
}

// GetConfig возвращает текущую конфигурацию
func (cs *configService) GetConfig() *Config {
	return cs.config
}
