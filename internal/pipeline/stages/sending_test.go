package stages

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestNetworkSendingStage_Creation(t *testing.T) {
	stage := NewNetworkSendingStage("test-sender")

	if stage.Name() != "test-sender" {
		t.Errorf("Expected name 'test-sender', got '%s'", stage.Name())
	}

	if stage.protocol != "udp" {
		t.Errorf("Expected default protocol 'udp', got '%s'", stage.protocol)
	}

	if len(stage.destinations) != 1 || stage.destinations[0] != "127.0.0.1:514" {
		t.Errorf("Expected default destination '127.0.0.1:514', got %v", stage.destinations)
	}

	if stage.GetSentCount() != 0 {
		t.Errorf("Initial sent count should be 0, got %d", stage.GetSentCount())
	}
}

func TestNetworkSendingStage_SetDestinations(t *testing.T) {
	stage := NewNetworkSendingStage("test")

	// Валидные адреса
	destinations := []string{"127.0.0.1:1234", "192.168.1.1:5678"}
	err := stage.SetDestinations(destinations)
	if err != nil {
		t.Errorf("SetDestinations should not error: %v", err)
	}

	// Пустой список
	err = stage.SetDestinations([]string{})
	if err == nil {
		t.Error("SetDestinations with empty slice should return error")
	}

	// Невалидный адрес
	err = stage.SetDestinations([]string{"invalid-address"})
	if err == nil {
		t.Error("SetDestinations with invalid address should return error")
	}
}

func TestNetworkSendingStage_SetProtocol(t *testing.T) {
	stage := NewNetworkSendingStage("test")

	// Валидные протоколы
	err := stage.SetProtocol("tcp")
	if err != nil {
		t.Errorf("SetProtocol('tcp') should not error: %v", err)
	}

	err = stage.SetProtocol("udp")
	if err != nil {
		t.Errorf("SetProtocol('udp') should not error: %v", err)
	}

	// Невалидный протокол
	err = stage.SetProtocol("http")
	if err == nil {
		t.Error("SetProtocol('http') should return error")
	}
}

// Mock UDP сервер для тестирования
func startMockUDPServer(t *testing.T, addr string) (net.PacketConn, chan []byte) {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		t.Fatalf("Failed to start mock UDP server: %v", err)
	}

	received := make(chan []byte, 100) // Больший буфер

	go func() {
		defer close(received) // Закрываем канал при завершении

		buffer := make([]byte, 1024)
		for {
			// Устанавливаем timeout для чтения
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

			n, _, err := conn.ReadFrom(buffer)
			if err != nil {
				// Проверяем timeout error
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // Продолжаем чтение
				}
				fmt.Printf("UDP server read error: %v\n", err)
				return // Соединение закрыто или другая ошибка
			}

			// fmt.Printf("UDP server received %d bytes from %s: %s\n", n, addr, string(buffer[:n]))

			data := make([]byte, n)
			copy(data, buffer[:n])

			select {
			case received <- data:
				// Отправили в канал
			default:
				fmt.Printf("UDP server: received channel full, dropping message\n")
			}
		}
	}()

	// Даем серверу время для запуска
	time.Sleep(50 * time.Millisecond)

	return conn, received
}

func TestNetworkSendingStage_UDPSending(t *testing.T) {
	// Запускаем mock UDP сервер
	server, received := startMockUDPServer(t, "127.0.0.1:0")
	defer server.Close()

	serverAddr := server.LocalAddr().String()

	// Создаем и настраиваем стадию отправки
	stage := NewNetworkSendingStage("udp-test")
	stage.SetDestinations([]string{serverAddr})
	stage.SetProtocol("udp")

	// Подготавливаем тестовые данные
	testData := &SerializedData{
		Data:        []byte("test udp message"),
		EventID:     "test-event-1",
		Size:        17,
		Destination: "", // Будет использован serverAddr из конфигурации
		Protocol:    "", // Будет использован udp из конфигурации
	}

	inputChan := make(chan *SerializedData, 1)
	inputChan <- testData
	close(inputChan) // Закрываем чтобы стадия завершилась

	ctx := context.Background()

	// Запускаем стадию отправки
	err := stage.Run(ctx, inputChan)
	if err != nil {
		t.Errorf("Run() should not error: %v", err)
	}

	// Проверяем что данные получены сервером
	select {
	case receivedData := <-received:
		if string(receivedData) != "test udp message" {
			t.Errorf("Expected 'test udp message', got '%s'", string(receivedData))
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for UDP data")
	}

	// Проверяем метрики
	if stage.GetSentCount() != 1 {
		t.Errorf("Expected sent count 1, got %d", stage.GetSentCount())
	}

	if stage.GetFailedCount() != 0 {
		t.Errorf("Expected failed count 0, got %d", stage.GetFailedCount())
	}
}

func TestNetworkSendingStage_SendError(t *testing.T) {
	stage := NewNetworkSendingStage("error-test")
	stage.SetDestinations([]string{"255.255.255.255:99999"}) // Невалидный адрес
	stage.SetProtocol("udp")

	testData := &SerializedData{
		Data:    []byte("test message"),
		EventID: "test-event-1",
		Size:    12,
	}

	inputChan := make(chan *SerializedData, 1)
	inputChan <- testData
	close(inputChan)

	ctx := context.Background()

	// Запускаем стадию - ошибок быть не должно, только failed count увеличится
	err := stage.Run(ctx, inputChan)
	if err != nil {
		t.Errorf("Run() should not error even with send failures: %v", err)
	}

	// Проверяем что произошла ошибка отправки
	if stage.GetFailedCount() == 0 {
		t.Error("Expected failed count > 0 for invalid destination")
	}

	if stage.GetSentCount() != 0 {
		t.Errorf("Expected sent count 0, got %d", stage.GetSentCount())
	}
}

func TestNetworkSendingStage_ContextCancellation(t *testing.T) {
	stage := NewNetworkSendingStage("cancel-test")

	ctx, cancel := context.WithCancel(context.Background())
	inputChan := make(chan *SerializedData) // Не закрываем канал

	// Запускаем стадию в горутине
	errChan := make(chan error, 1)
	go func() {
		errChan <- stage.Run(ctx, inputChan)
	}()

	// Отменяем контекст
	cancel()

	// Проверяем быстрое завершение
	select {
	case err := <-errChan:
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Stage did not stop within 100ms after context cancellation")
	}
}

func BenchmarkNetworkSending(b *testing.B) {
	stage := NewNetworkSendingStage("bench")

	// Используем реальный UDP адрес для бенчмарка
	stage.SetDestinations([]string{"127.0.0.1:1234"})
	stage.SetProtocol("udp")

	testData := &SerializedData{
		Data:    []byte("benchmark test message"),
		EventID: "bench-event",
		Size:    23,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stage.SendData(testData) // Напрямую вызываем метод отправки
	}
}
