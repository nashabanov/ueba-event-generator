package stages

import (
	"context"
	"fmt"

	// "runtime"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
	"github.com/nashabanov/ueba-event-generator/internal/metrics"
	"github.com/nashabanov/ueba-event-generator/internal/workers"
)

// EventGenerationJob - задача для Worker Pool
type EventGenerationJob struct {
	stage *EventGenerationStage
	out   chan<- *SerializedData
}

// Execute реализует интерфейс workers.Job
func (job *EventGenerationJob) Execute() error {
	startTime := time.Now()

	err := job.stage.generateAndSendEvent(context.Background(), job.out)

	processingTime := time.Since(startTime)
	metrics.GetGlobalMetrics().RecordProcessingTime(processingTime)

	return err
}

// EventGenerationStage реализует GenerationStage
type EventGenerationStage struct {
	name            string
	eventTypes      []event.EventType
	eventsPerSecond int

	// НОВЫЕ ПОЛЯ для Worker Pool
	workerPool *workers.WorkerPool
	ticker     *time.Ticker
}

// NewEventGenerationStage создает новую стадию генерации
func NewEventGenerationStage(name string, eventsPerSecond int) *EventGenerationStage {
	queueSize := eventsPerSecond * 2
	if queueSize <= 1000 {
		queueSize = 1000
	}

	// workerCount := runtime.NumCPU() * 3
	workerPool := workers.NewWorkerPool(0, queueSize)
	workerPool.SetPoolType("generation")

	// fmt.Printf("🔧 Creating EventGenerationStage: EPS=%d, QueueSize=%d, Workers=%d\n",
	// 	eventsPerSecond, queueSize, workerCount) //

	return &EventGenerationStage{
		name:            name,
		eventsPerSecond: eventsPerSecond,
		eventTypes:      []event.EventType{event.EventTypeNetflow}, // Проверить что константа определена
		workerPool:      workers.NewWorkerPool(0, queueSize),
	}
}

func (g *EventGenerationStage) Name() string {
	return g.name
}

func (g *EventGenerationStage) Run(ctx context.Context, out chan<- *SerializedData) error {
	measuredTickerEPS := 1160 // Проверенный предел системы
	safetyMargin := 0.95      // 5% запас на вариативность

	effectiveTickerEPS := int(float64(measuredTickerEPS) * safetyMargin) // 1102 EPS
	batchSize := (g.eventsPerSecond + effectiveTickerEPS - 1) / effectiveTickerEPS

	if g.eventsPerSecond >= 50000 && batchSize > 100 {
		// Для очень высоких нагрузок используем более консервативный подход
		effectiveTickerEPS = 1000
		batchSize = g.eventsPerSecond / effectiveTickerEPS
	}

	interval := time.Second / time.Duration(effectiveTickerEPS)
	expectedEPS := effectiveTickerEPS * batchSize
	accuracy := float64(expectedEPS) / float64(g.eventsPerSecond) * 100

	// fmt.Printf("🚀 Calibrated batch generation:\n")
	// fmt.Printf("   Target EPS: %d\n", g.eventsPerSecond)
	// fmt.Printf("   Measured system limit: %d EPS\n", measuredTickerEPS)
	// fmt.Printf("   Effective ticker EPS: %d (with %.0f%% safety margin)\n",
	// 	effectiveTickerEPS, (1-safetyMargin)*100)
	// fmt.Printf("   Batch size: %d events per tick\n", batchSize)
	// fmt.Printf("   Expected EPS: %d\n", expectedEPS)
	// fmt.Printf("   Expected accuracy: %.1f%%\n", accuracy)

	// Предупреждение если точность может быть низкой
	if accuracy < 95.0 {
		fmt.Printf("⚠️  Low accuracy warning: consider increasing batch size\n")
	} else if accuracy > 105.0 {
		fmt.Printf("⚠️  Over-target warning: batch size may be too large\n")
	}

	g.ticker = time.NewTicker(interval)
	defer g.ticker.Stop()

	g.workerPool.Start(ctx)
	defer g.workerPool.Stop()

	globalMetrics := metrics.GetGlobalMetrics()

	// Счетчики для диагностики
	tickCount := 0
	totalJobsSubmitted := 0
	totalJobsRejected := 0
	startTime := time.Now()

	for {
		select {
		case <-g.ticker.C:
			tickCount++

			// ГЕНЕРИРУЕМ BATCH СОБЫТИЙ за один тик
			batchSubmitted := 0
			batchRejected := 0

			for i := 0; i < batchSize; i++ {
				job := &EventGenerationJob{
					stage: g,
					out:   out,
				}

				if g.workerPool.Submit(job) {
					batchSubmitted++
					totalJobsSubmitted++
				} else {
					batchRejected++
					totalJobsRejected++
					globalMetrics.IncrementDropped()

					// Если очередь полная, прерываем batch
					if batchRejected > batchSize/2 {
						break
					}
				}
			}

			// Диагностика каждые 1000 тиков (или если есть проблемы)
			if tickCount%1000 == 0 || batchRejected > 0 {
				elapsed := time.Since(startTime).Seconds()
				actualTickRate := float64(tickCount) / elapsed
				expectedEvents := tickCount * batchSize
				actualEvents := totalJobsSubmitted

				fmt.Printf("🔍 Tick %d: rate=%.1f/sec, batch=%d/%d, total_jobs=%d/%d (%.1f%% efficiency)\n",
					tickCount, actualTickRate, batchSubmitted, batchSize,
					actualEvents, expectedEvents, float64(actualEvents)/float64(expectedEvents)*100)
			}

		case <-ctx.Done():
			// ✅ ФИНАЛЬНАЯ статистика
			// elapsed := time.Since(startTime).Seconds()
			// actualTickRate := float64(tickCount) / elapsed
			// expectedEvents := tickCount * batchSize
			// actualEPS := float64(totalJobsSubmitted) / elapsed

			// fmt.Printf("🛑 Batch generation stopped:\n")
			// fmt.Printf("   Runtime: %.1fs\n", elapsed)
			// fmt.Printf("   Ticks: %d (rate: %.1f/sec)\n", tickCount, actualTickRate)
			// fmt.Printf("   Jobs: submitted=%d, rejected=%d\n", totalJobsSubmitted, totalJobsRejected)
			// fmt.Printf("   Expected events: %d, Actual EPS: %.1f\n", expectedEvents, actualEPS)
			// fmt.Printf("   Efficiency: %.1f%%\n", float64(totalJobsSubmitted)/float64(expectedEvents)*100)

			return ctx.Err()
		}
	}
}

// generateAndSendEvent используем существующие функции из пакета event
func (g *EventGenerationStage) generateAndSendEvent(ctx context.Context, out chan<- *SerializedData) error {
	eventType := g.eventTypes[0]

	var evt event.Event
	switch eventType {
	case event.EventTypeNetflow:
		evt = event.NewNetflowEvent()
	case event.EventTypeSyslog:
		evt = event.NewSyslogEvent()
	default:
		return fmt.Errorf("unsupported event type: %v", eventType)
	}

	jsonData, err := evt.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	serializedData := NewSerializedData(jsonData, evt.Type(), evt.GetID())

	globalMetrics := metrics.GetGlobalMetrics()

	select {
	case out <- serializedData:
		globalMetrics.IncrementGenerated()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Остальные методы без изменений...
func (g *EventGenerationStage) SetEventRate(eventsPerSecond int) error {
	if eventsPerSecond <= 0 {
		return fmt.Errorf("events per second must be positive, got: %d", eventsPerSecond)
	}
	g.eventsPerSecond = eventsPerSecond
	return nil
}

func (g *EventGenerationStage) SetEventTypes(types []event.EventType) error {
	if len(types) == 0 {
		return fmt.Errorf("event types cannot be empty")
	}
	g.eventTypes = types
	return nil
}

func (g *EventGenerationStage) GetGeneratedCount() uint64 {
	generated, _, _, _ := metrics.GetGlobalMetrics().GetStats()
	return generated
}

func (g *EventGenerationStage) GetFailedCount() uint64 {
	_, _, failed, _ := metrics.GetGlobalMetrics().GetStats()
	return failed
}
