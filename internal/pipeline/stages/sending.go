package stages

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// NetworkSendingStage реализует SendingStage для отправки по сети
type NetworkSendingStage struct {
	name         string
	destinations []string // ["127.0.0.1:514", "10.0.0.1:514"]
	protocol     string   // "udp" или "tcp"

	// Метрики (atomic для thread-safety)
	sentCount   uint64
	failedCount uint64

	// Сетевые соединения (для TCP)
	connections map[string]net.Conn
}

// NewNetworkSendingStage создает новую стадию отправки
func NewNetworkSendingStage(name string) *NetworkSendingStage {
	return &NetworkSendingStage{
		name:         name,
		destinations: []string{"127.0.0.1:514"}, // По умолчанию локальный syslog
		protocol:     "udp",                     // По умолчанию UDP
		connections:  make(map[string]net.Conn),
	}
}

func (s *NetworkSendingStage) Name() string {
	return s.name
}

func (s *NetworkSendingStage) Run(ctx context.Context, in <-chan *SerializedData) error {
	fmt.Printf("Sending stage '%s' started: protocol=%s, destinations=%v\n",
		s.name, s.protocol, s.destinations)

	defer s.closeConnections() // Закрываем соединения при завершении

	for {
		select {
		case serializedData, ok := <-in:
			if !ok {
				// Входной канал закрыт - завершаемся
				fmt.Printf("Sending stage '%s' stopped. Sent: %d, Failed: %d\n",
					s.name, s.GetSentCount(), s.GetFailedCount())
				return nil
			}

			// Отправляем данные
			if err := s.SendData(serializedData); err != nil {
				atomic.AddUint64(&s.failedCount, 1)
				fmt.Printf("Send error: %v\n", err)
				continue
			}

			atomic.AddUint64(&s.sentCount, 1)

		case <-ctx.Done():
			fmt.Printf("Sending stage '%s' cancelled. Sent: %d, Failed: %d\n",
				s.name, s.GetSentCount(), s.GetFailedCount())
			return ctx.Err()
		}
	}
}

// SendData отправляет данные по сети
func (s *NetworkSendingStage) SendData(data *SerializedData) error {
	// Определяем куда отправлять (приоритет: метаданные события, затем конфигурация стадии)
	destination := data.Destination
	if destination == "" && len(s.destinations) > 0 {
		destination = s.destinations[0] // Используем первый из списка
	}

	protocol := data.Protocol
	if protocol == "" {
		protocol = s.protocol
	}

	switch protocol {
	case "udp":
		return s.sendUDP(destination, data.Data)
	case "tcp":
		return s.sendTCP(destination, data.Data)
	default:
		return fmt.Errorf("unsupported protocol: %s", protocol)
	}
}

// sendUDP отправляет данные через UDP (stateless)
func (s *NetworkSendingStage) sendUDP(destination string, data []byte) error {
	// UDP - каждый раз новое соединение
	conn, err := net.Dial("udp", destination)
	if err != nil {
		return fmt.Errorf("failed to dial UDP %s: %w", destination, err)
	}
	defer conn.Close()

	// Устанавливаем timeout для записи
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write UDP data to %s: %w", destination, err)
	}

	return nil
}

// sendTCP отправляет данные через TCP (с переиспользованием соединений)
func (s *NetworkSendingStage) sendTCP(destination string, data []byte) error {
	// Проверяем есть ли уже соединение
	conn, exists := s.connections[destination]
	if !exists || conn == nil {
		// Создаем новое соединение
		var err error
		conn, err = net.Dial("tcp", destination)
		if err != nil {
			return fmt.Errorf("failed to dial TCP %s: %w", destination, err)
		}
		s.connections[destination] = conn
	}

	// Устанавливаем timeout для записи
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	_, err := conn.Write(data)
	if err != nil {
		// Соединение сломалось - удаляем из пула
		conn.Close()
		delete(s.connections, destination)
		return fmt.Errorf("failed to write TCP data to %s: %w", destination, err)
	}

	return nil
}

// closeConnections закрывает все TCP соединения
func (s *NetworkSendingStage) closeConnections() {
	for destination, conn := range s.connections {
		if conn != nil {
			conn.Close()
			fmt.Printf("Closed TCP connection to %s\n", destination)
		}
	}
	s.connections = make(map[string]net.Conn)
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

	s.destinations = destinations
	return nil
}

func (s *NetworkSendingStage) SetProtocol(protocol string) error {
	if protocol != "udp" && protocol != "tcp" {
		return fmt.Errorf("unsupported protocol: %s (supported: udp, tcp)", protocol)
	}

	// Если меняем протокол - закрываем старые TCP соединения
	if s.protocol == "tcp" && protocol != "tcp" {
		s.closeConnections()
	}

	s.protocol = protocol
	return nil
}

// Методы метрик
func (s *NetworkSendingStage) GetSentCount() uint64 {
	return atomic.LoadUint64(&s.sentCount)
}

func (s *NetworkSendingStage) GetFailedCount() uint64 {
	return atomic.LoadUint64(&s.failedCount)
}
