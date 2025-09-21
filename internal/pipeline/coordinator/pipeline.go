package coordinator

import (
	"context"
	"fmt"
	"sync"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
)

// Pipeline - интерфейс для пайплайна
type Pipeline interface {
	// Lifecicle - жизненный цикл
	Start(ctx context.Context) error
	Stop() error

	// Configuration - настройка
	AddStage(stage Stage) error

	// Мonitoring - мониторинг
	GetStatus() PipelineStatus
}

// Stage - интерфейс для этапа пайплайна
type Stage interface {
	Name() string
	Run(ctx context.Context, in <-chan event.Event, out chan<- event.Event) error
}

type PipelineStatus int

const (
	Stopped PipelineStatus = iota
	Starting
	Running
	Stopping
	Error
)

// pipelineImpl - реализация пайплайна
type pipelineImpl struct {
	// Состояние
	status PipelineStatus
	mu     sync.RWMutex // защищает изменения статуса

	stages   []Stage            // список этапов
	channels []chan event.Event // канал между стадиями

	// Входной и выходной каналы
	inputChan  chan event.Event
	outputChan chan event.Event

	// Управление жизненным циклом
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Конфигурация
	bufferSize int
}

// NewPipeline создает новый экземпляр пайплайна
func NewPipeline(bufferSize int) Pipeline {
	return &pipelineImpl{
		status:   Stopped,
		stages:   make([]Stage, 0),
		channels: make([]chan event.Event, 0),

		inputChan:  make(chan event.Event, bufferSize),
		outputChan: make(chan event.Event, bufferSize),

		bufferSize: bufferSize,
	}
}

// GetStatus возвращает текущий статус пайплайна
func (p *pipelineImpl) GetStatus() PipelineStatus {
	p.mu.RLock() // Читаем - используем RLock
	defer p.mu.RUnlock()
	return p.status
}

// Start запускает пайплайн
func (p *pipelineImpl) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.status != Stopped {
		return fmt.Errorf("cannot start, pipeline status: %v", p.status)
	}

	p.status = Starting

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.createChannels()

	p.startStages()

	p.status = Running
	fmt.Printf("Pipeline started with %d stages\n", len(p.stages))
	return nil
}

// Stop останавливает пайплайн
func (p *pipelineImpl) Stop() error {
	p.mu.Lock()
	if p.status != Running {
		p.mu.Unlock()
		return fmt.Errorf("cannot stop, pipeline status: %v", p.status)
	}

	p.status = Stopping
	p.mu.Unlock()

	fmt.Printf("Stopping pipeline...\n")

	if p.cancel != nil {
		p.cancel()
	}

	close(p.inputChan)

	p.wg.Wait()

	p.closeChannels()

	p.mu.Lock()
	p.status = Stopped
	p.mu.Unlock()

	fmt.Printf("Pipeline stopped successfully\n")
	return nil
}

// AddStage добавляет новый этап в пайплайн
func (p *pipelineImpl) AddStage(stage Stage) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.status != Stopped {
		return fmt.Errorf("cannot add stage, pipeline status: %v", p.status)
	}

	p.stages = append(p.stages, stage)
	return nil
}

// createChannels создает каналы для передачи данных между этапами
func (p *pipelineImpl) createChannels() {
	stageCount := len(p.stages)

	if stageCount == 0 {
		return
	}

	if stageCount == 1 {
		p.channels = make([]chan event.Event, 0)
		return
	}

	p.channels = make([]chan event.Event, stageCount-1)
	for i := 0; i < stageCount-1; i++ {
		p.channels[i] = make(chan event.Event, p.bufferSize)
		fmt.Printf("Created intermediate channel %d with buffer size %d\n", i, p.bufferSize)
	}
}

// startStage запускает все этапы пайплайна
func (p *pipelineImpl) startStages() {
	for i, stage := range p.stages {
		inputChan := p.getInputChannel(i)
		outputChan := p.getOutputChannel(i)

		p.wg.Add(1)

		go func(s Stage, in <-chan event.Event, out chan<- event.Event, index int) {
			defer p.wg.Done()

			fmt.Printf("Stage '%s' started\n", s.Name())

			err := s.Run(p.ctx, in, out)
			if err != nil {
				fmt.Printf("Stage '%s' finished with error: %v\n", s.Name(), err)
			} else {
				fmt.Printf("Stage '%s' finished successfully\n", s.Name())
			}
		}(stage, inputChan, outputChan, i)
	}
}

// getInputChannel возвращает входной канал для указанной стадии
func (p *pipelineImpl) getInputChannel(stageIndex int) <-chan event.Event {
	if stageIndex == 0 {
		return p.inputChan // Первая стадия читает из входа
	}
	return p.channels[stageIndex-1] // Остальные из промежуточных
}

// getOutputChannel возвращает выходной канал для указанной стадии
func (p *pipelineImpl) getOutputChannel(stageIndex int) chan<- event.Event {
	if stageIndex == len(p.stages)-1 {
		return p.outputChan // Последняя пишет в выход
	}
	return p.channels[stageIndex] // Остальные в промежуточные
}

// closeChannels закрывает все каналы пайплайна
func (p *pipelineImpl) closeChannels() {
	for _, ch := range p.channels {
		close(ch)
	}
	close(p.outputChan)
	fmt.Printf("Closed output channel\n")
}
