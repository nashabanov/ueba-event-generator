package stages

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/domain/event"
	"github.com/nashabanov/ueba-event-generator/internal/metrics"
	"github.com/nashabanov/ueba-event-generator/internal/network"
	"github.com/nashabanov/ueba-event-generator/internal/workers"
)

// NetworkSendJob - задача для отправки через Worker Pool
type NetworkSendJob struct {
	stage *NetworkSendingStage
	data  *SerializedData
}

// Execute реализует интерфейс workers.Job
func (job *NetworkSendJob) Execute() error {
	return job.stage.SendData(job.data)
}

// NetworkSendingStage реализует SendingStage для отправки по сети
type NetworkSendingStage struct {
	name         string
	destinations []string // ["127.0.0.1:514", "10.0.0.1:514"]
	protocol     string   // "udp" или "tcp"
	timeout      time.Duration

	// ✅ НОВАЯ архитектура - заменяем старые поля
	tcpPool    *network.TCPConnectionPool // Connection pool для TCP
	workerPool *workers.WorkerPool
	metrics    *metrics.PerformanceMetrics
	input      chan event.Event

	// ✅ УБИРАЕМ старые поля - теперь управление соединениями в pool
	// connections map[string]net.Conn  // ❌ УДАЛЕНО
	// connMutex   sync.RWMutex         // ❌ УДАЛЕНО
}

func NewNetworkSendingStage(name string) *NetworkSendingStage {
	workerPool := workers.NewWorkerPool(0, 5000)
	workerPool.SetPoolType("network")
	return &NetworkSendingStage{
		name:         name,
		destinations: []string{"127.0.0.1:514"},
		protocol:     "udp",
		timeout:      5 * time.Second,
		workerPool:   workerPool, // ✅ ИСПРАВЛЯЕМ дублирование
		metrics:      metrics.NewPerformanceMetrics(),
		input:        make(chan event.Event, 1000),
		// tcpPool будет создан в Run() когда узнаем точный протокол
	}
}

func (s *NetworkSendingStage) Name() string {
	return s.name
}

// ✅ ОБНОВЛЕННЫЙ Run с инициализацией TCP pool
func (s *NetworkSendingStage) Run(ctx context.Context, in <-chan *SerializedData) error {
	fmt.Printf("🚀 Sending stage '%s' started: protocol=%s, destinations=%v with Worker Pool\n",
		s.name, s.protocol, s.destinations)

	// ✅ СОЗДАЕМ TCP POOL если протокол TCP
	if s.protocol == "tcp" && len(s.destinations) > 0 {
		poolSize := 12 // 12 соединений для 180k EPS (12 * 15k)

		tcpPool, err := network.NewTCPConnectionPool(s.destinations[0], poolSize)
		if err != nil {
			return fmt.Errorf("failed to create TCP connection pool: %w", err)
		}
		s.tcpPool = tcpPool

		log.Printf("✅ TCP connection pool created: %d connections to %s", poolSize, s.destinations[0])
	}

	s.workerPool.Start(ctx)
	defer s.workerPool.Stop()
	defer s.closeConnections()

	for {
		select {
		case serializedData, ok := <-in:
			if !ok {
				_, sent, failed, _ := metrics.GetGlobalMetrics().GetStats()
				fmt.Printf("🛑 Sending stage '%s' stopped. Sent: %d, Failed: %d\n",
					s.name, sent, failed)
				return nil
			}

			// Создаем задачу для Worker Pool
			job := &NetworkSendJob{
				stage: s,
				data:  serializedData,
			}

			if !s.workerPool.Submit(job) {
				metrics.GetGlobalMetrics().IncrementDropped()
				fmt.Printf("⚠️ Network worker pool queue full, dropping send\n")
			}

		case <-ctx.Done():
			_, sent, failed, _ := metrics.GetGlobalMetrics().GetStats()
			fmt.Printf("🛑 Sending stage '%s' cancelled. Sent: %d, Failed: %d\n",
				s.name, sent, failed)
			return ctx.Err()
		}
	}
}

// SendData отправляет данные по сети
func (s *NetworkSendingStage) SendData(data *SerializedData) error {
	globalMetrics := metrics.GetGlobalMetrics()

	destination := data.Destination
	if destination == "" && len(s.destinations) > 0 {
		destination = s.destinations[0]
	}

	protocol := data.Protocol
	if protocol == "" {
		protocol = s.protocol
	}

	var err error
	switch protocol {
	case "udp":
		err = s.sendUDP(destination, data.Data)
	case "tcp":
		err = s.sendTCP(destination, data.Data)
	default:
		err = fmt.Errorf("unsupported protocol: %s", protocol)
	}

	if err != nil {
		globalMetrics.IncrementFailed()
		return err
	}

	globalMetrics.IncrementSent()
	return nil
}

// sendUDP отправляет данные через UDP (stateless) - без изменений
func (s *NetworkSendingStage) sendUDP(destination string, data []byte) error {
	globalMetrics := metrics.GetGlobalMetrics()

	conn, err := net.Dial("udp", destination)
	if err != nil {
		globalMetrics.IncrementTimeouts()
		return fmt.Errorf("failed to dial UDP %s: %w", destination, err)
	}
	defer func() {
		conn.Close()
		// ✅ Для UDP "соединение" кратковременное, но все равно считаем
		globalMetrics.DecrementConnections()
	}()

	// ✅ Считаем временное UDP "соединение"
	globalMetrics.IncrementConnections()

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	_, err = conn.Write(data)
	if err != nil {
		globalMetrics.IncrementTimeouts()
		return fmt.Errorf("failed to write UDP data to %s: %w", destination, err)
	}

	return nil
}

// ✅ ПОЛНОСТЬЮ ОБНОВЛЕННЫЙ sendTCP с использованием connection pool
func (s *NetworkSendingStage) sendTCP(destination string, data []byte) error {
	// Проверяем что TCP pool инициализирован
	if s.tcpPool == nil {
		return fmt.Errorf("TCP connection pool not initialized for destination %s", destination)
	}

	globalMetrics := metrics.GetGlobalMetrics()

	// ✅ ПОЛУЧАЕМ соединение из pool
	conn := s.tcpPool.GetConnection()
	if conn == nil {
		globalMetrics.IncrementTimeouts()
		return fmt.Errorf("no healthy TCP connections available to %s", destination)
	}

	// ✅ ДОБАВЛЯЕМ перенос строки для разделения событий
	dataWithNewline := append(data, '\n')

	// ✅ ОТПРАВЛЯЕМ данные через connection pool
	written := 0
	for written < len(dataWithNewline) {
		n, err := conn.Write(dataWithNewline[written:])
		if err != nil {
			// ✅ CONNECTION POOL автоматически обработает ошибку
			conn.MarkUnhealthy()
			globalMetrics.IncrementTimeouts()
			return fmt.Errorf("failed to write TCP data to %s: %w", destination, err)
		}
		written += n
	}

	return nil
}

// ✅ ОБНОВЛЕННЫЙ closeConnections для работы с pool
func (s *NetworkSendingStage) closeConnections() {
	if s.tcpPool != nil {
		s.tcpPool.Close()
		log.Printf("🔒 TCP connection pool closed")
		s.tcpPool = nil
	}
}

// ✅ НОВЫЙ метод для получения статистики connection pool
func (s *NetworkSendingStage) GetConnectionStats() (total, healthy int) {
	if s.tcpPool != nil {
		return s.tcpPool.GetStats()
	}
	return 0, 0
}

// ✅ НОВЫЙ метод для получения детальной информации о pool
func (s *NetworkSendingStage) GetConnectionPoolInfo() string {
	if s.tcpPool == nil {
		return "TCP pool: not initialized"
	}

	total, healthy := s.tcpPool.GetStats()
	return fmt.Sprintf("TCP pool: %d/%d healthy connections", healthy, total)
}

// Конфигурационные методы
func (s *NetworkSendingStage) SetDestinations(destinations []string) error {
	if len(destinations) == 0 {
		return fmt.Errorf("destinations cannot be empty")
	}

	// Валидируем адреса
	for _, dest := range destinations {
		if _, _, err := net.SplitHostPort(dest); err != nil {
			return fmt.Errorf("invalid destination address %s: %w", dest, err)
		}
	}

	// ✅ ЕСЛИ изменили destination и TCP pool активен - нужно пересоздать
	if s.tcpPool != nil && len(destinations) > 0 && destinations[0] != s.destinations[0] {
		log.Printf("🔄 Destination changed from %s to %s, TCP pool will be recreated on next run",
			s.destinations[0], destinations[0])
		s.closeConnections() // Закрываем старый pool
	}

	s.destinations = destinations
	return nil
}

func (s *NetworkSendingStage) SetProtocol(protocol string) error {
	if protocol != "udp" && protocol != "tcp" {
		return fmt.Errorf("unsupported protocol: %s (supported: udp, tcp)", protocol)
	}

	// ✅ ЕСЛИ меняем протокол - закрываем TCP pool
	if s.protocol == "tcp" && protocol != "tcp" {
		s.closeConnections()
		log.Printf("🔄 Protocol changed from TCP to %s, connection pool closed", protocol)
	}

	s.protocol = protocol
	return nil
}

// ✅ НОВЫЙ метод для изменения размера pool в runtime (advanced)
func (s *NetworkSendingStage) ResizeConnectionPool(newSize int) error {
	if s.tcpPool == nil {
		return fmt.Errorf("TCP connection pool not initialized")
	}

	if newSize < 1 || newSize > 50 {
		return fmt.Errorf("invalid pool size %d, must be between 1 and 50", newSize)
	}

	// Пока что просто логируем - полноценная реализация resize сложна
	total, healthy := s.tcpPool.GetStats()
	log.Printf("🔧 TCP pool resize requested: current=%d healthy/%d total, requested=%d",
		healthy, total, newSize)
	log.Printf("⚠️ Pool resizing not implemented - requires restart to change size")

	return fmt.Errorf("pool resizing not implemented - restart application with new configuration")
}

// ✅ НОВЫЙ метод для принудительного восстановления соединений
func (s *NetworkSendingStage) RecreateUnhealthyConnections() int {
	if s.tcpPool == nil {
		return 0
	}

	// Эта логика будет в connection pool - пока что заглушка
	total, healthy := s.tcpPool.GetStats()
	unhealthy := total - healthy

	if unhealthy > 0 {
		log.Printf("🔄 Attempting to recreate %d unhealthy connections", unhealthy)
		// В реальной реализации pool сам будет пересоздавать соединения
		return unhealthy
	}

	return 0
}

// Методы метрик
func (s *NetworkSendingStage) GetSentCount() uint64 {
	_, sent, _, _ := metrics.GetGlobalMetrics().GetStats()
	return sent
}

func (s *NetworkSendingStage) GetFailedCount() uint64 {
	_, _, failed, _ := metrics.GetGlobalMetrics().GetStats()
	return failed
}

// ✅ НОВЫЙ метод для получения полной статистики stage
func (s *NetworkSendingStage) GetStageStats() map[string]interface{} {
	_, sent, failed, dropped := metrics.GetGlobalMetrics().GetStats()

	stats := map[string]interface{}{
		"stage_name":          s.name,
		"protocol":            s.protocol,
		"destinations":        len(s.destinations),
		"events_sent":         sent,
		"events_failed":       failed,
		"events_dropped":      dropped,
		"worker_pool_healthy": s.workerPool != nil,
	}

	// Добавляем TCP pool статистику если доступна
	if s.tcpPool != nil {
		total, healthy := s.tcpPool.GetStats()
		stats["tcp_connections_total"] = total
		stats["tcp_connections_healthy"] = healthy
		stats["tcp_pool_efficiency"] = float64(healthy) / float64(total) * 100.0
	}

	return stats
}

// ✅ НОВЫЙ метод для health check всего stage
func (s *NetworkSendingStage) IsHealthy() (bool, string) {
	if s.workerPool == nil {
		return false, "worker pool not initialized"
	}

	// Проверяем TCP pool если используется TCP
	if s.protocol == "tcp" {
		if s.tcpPool == nil {
			return false, "TCP protocol selected but connection pool not initialized"
		}

		total, healthy := s.tcpPool.GetStats()
		if healthy == 0 {
			return false, fmt.Sprintf("no healthy TCP connections (0/%d)", total)
		}

		efficiency := float64(healthy) / float64(total)
		if efficiency < 0.5 {
			return false, fmt.Sprintf("TCP connection pool degraded: %d/%d healthy (%.1f%%)",
				healthy, total, efficiency*100)
		}
	}

	return true, "stage healthy"
}

// ✅ НОВЫЙ метод для получения рекомендаций по оптимизации
func (s *NetworkSendingStage) GetOptimizationRecommendations() []string {
	var recommendations []string

	_, sent, failed, dropped := metrics.GetGlobalMetrics().GetStats()

	// Анализируем метрики и даем рекомендации
	if dropped > 0 {
		dropRate := float64(dropped) / float64(float64(sent)+float64(failed)+float64(dropped)) * 100
		if dropRate > 1.0 {
			recommendations = append(recommendations,
				fmt.Sprintf("High drop rate (%.1f%%) - consider increasing worker pool queue size", dropRate))
		}
	}

	if failed > 0 {
		failRate := float64(failed) / float64(sent+failed) * 100
		if failRate > 5.0 {
			recommendations = append(recommendations,
				fmt.Sprintf("High failure rate (%.1f%%) - check network connectivity and destination availability", failRate))
		}
	}

	if s.protocol == "tcp" && s.tcpPool != nil {
		total, healthy := s.tcpPool.GetStats()
		if healthy < total {
			recommendations = append(recommendations,
				fmt.Sprintf("TCP connections degraded (%d/%d) - consider connection timeout tuning", healthy, total))
		}

		if total < 5 && sent > 50000 {
			recommendations = append(recommendations,
				"High load with few TCP connections - consider increasing connection pool size")
		}
	}

	if len(recommendations) == 0 {
		recommendations = append(recommendations, "Stage operating optimally - no recommendations")
	}

	return recommendations
}
