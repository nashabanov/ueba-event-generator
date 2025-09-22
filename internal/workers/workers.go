package workers

import (
	"context"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/metrics"
)

type Job interface {
	Execute() error
}

type WorkerPool struct {
	workerCount int            // Количество worker'ов
	jobQueue    chan Job       // Очередь задач
	quit        chan bool      // Сигнал для остановки
	wg          sync.WaitGroup // Ожидание завершения всех worker'ов
	poolType    string         // ✅ НОВОЕ ПОЛЕ для идентификации
}

func NewWorkerPool(workerCount int, queueSize int) *WorkerPool {
	// Автоматический расчет количества worker'ов
	if workerCount <= 0 {
		// CPU-intensive: NumCPU
		// I/O-intensive: NumCPU * 2-4
		workerCount = runtime.NumCPU() * 3
	}

	return &WorkerPool{
		workerCount: workerCount,
		jobQueue:    make(chan Job, queueSize), // Buffered channel
		quit:        make(chan bool),
		poolType:    "generic", // ✅ По умолчанию
	}
}

func (wp *WorkerPool) SetPoolType(poolType string) {
	wp.poolType = poolType
}

func (wp *WorkerPool) Start(ctx context.Context) {
	// Запускаем указанное количество worker'ов
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)         // Увеличиваем счетчик WaitGroup
		go wp.worker(i, ctx) // Каждый worker в своей goroutine
	}
}

func (wp *WorkerPool) worker(id int, ctx context.Context) {
	defer wp.wg.Done()

	globalMetrics := metrics.GetGlobalMetrics()
	globalMetrics.IncrementActiveWorkers()
	defer globalMetrics.DecrementActiveWorkers()

	log.Printf("🚀 Worker %d запущен", id)

	poolType := "unknown"
	if wp.poolType != "" {
		poolType = wp.poolType
	}

	log.Printf("🚀 Worker %s-%d запущен", poolType, id)

	for {
		select {
		case job := <-wp.jobQueue:
			if job != nil {
				startTime := time.Now()

				if err := job.Execute(); err != nil {
					log.Printf("❌ Worker %d: ошибка задачи: %v", id, err)
				} else {
					globalMetrics.IncrementCompletedJobs()
				}

				processingTime := time.Since(startTime)
				globalMetrics.RecordProcessingTime(processingTime)
			}

		case <-wp.quit:
			log.Printf("🛑 Worker %s-%d: останавливаюсь", poolType, id)
			return

		case <-ctx.Done():
			log.Printf("⏱️ Worker %s-%d: context отменен", poolType, id)
			return
		}
	}
}

func (wp *WorkerPool) Submit(job Job) bool {
	globalMetrics := metrics.GetGlobalMetrics()

	select {
	case wp.jobQueue <- job:
		globalMetrics.SetQueuedJobs(uint64(len(wp.jobQueue)))
		return true
	default:
		globalMetrics.IncrementRejectedJobs()
		return false
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.quit)     // Закрываем channel - все worker'ы получат сигнал
	wp.wg.Wait()       // Ждем пока все worker'ы завершатся
	close(wp.jobQueue) // Закрываем очередь
}
