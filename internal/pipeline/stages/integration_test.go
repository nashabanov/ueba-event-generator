package stages

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nashabanov/ueba-event-generator/internal/pipeline/coordinator"
)

func TestFullPipeline_Integration(t *testing.T) {
	// Запускаем mock UDP сервер для получения данных
	server, received := startMockUDPServer(t, "127.0.0.1:0")

	serverAddr := server.LocalAddr().String()
	fmt.Printf("Mock server listening on: %s\n", serverAddr)

	// Создаем стадии
	genStage := NewEventGenerationStage("test-generator", 5) // 5 events/sec
	sendStage := NewNetworkSendingStage("test-sender")
	sendStage.SetDestinations([]string{serverAddr})
	sendStage.SetProtocol("udp")

	// Создаем адаптеры
	genAdapter := NewGenerationAdapter(genStage, 10)
	sendAdapter := NewSendingAdapter(sendStage, 10)

	// Создаем и настраиваем pipeline
	pipeline := coordinator.NewPipeline(10)
	pipeline.AddStage(genAdapter)
	pipeline.AddStage(sendAdapter)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := pipeline.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// Собираем события ПЕРЕД остановкой pipeline
	var receivedMessages []string
	timeout := time.After(1500 * time.Millisecond)

	fmt.Printf("Starting message collection...\n")

collectLoop:
	for len(receivedMessages) < 8 { // Ожидаем до 8 сообщений
		select {
		case data, ok := <-received:
			if !ok {
				fmt.Println("Received channel closed")
				break collectLoop
			}
			receivedMessages = append(receivedMessages, string(data))
			fmt.Printf("✓ Received message %d: %s\n", len(receivedMessages), string(data))

		case <-timeout:
			fmt.Printf("Collection timeout reached, collected %d messages\n", len(receivedMessages))
			break collectLoop
		}
	}

	// ТЕПЕРЬ останавливаем pipeline
	fmt.Printf("Stopping pipeline after collecting %d messages...\n", len(receivedMessages))
	err = pipeline.Stop()
	if err != nil {
		t.Errorf("Failed to stop pipeline: %v", err)
	}

	// ТЕПЕРЬ закрываем сервер
	server.Close()

	// Проверяем результаты
	if len(receivedMessages) == 0 {
		t.Fatal("No messages received from pipeline")
	}

	// При 5 events/sec за 1 секунду должно быть 4-6 событий
	if len(receivedMessages) < 3 || len(receivedMessages) > 8 {
		t.Errorf("Expected 3-8 messages, got %d", len(receivedMessages))
	}

	// Проверяем метрики генерации
	if genStage.GetGeneratedCount() == 0 {
		t.Error("Generation stage generated 0 events")
	}

	// Проверяем метрики отправки
	if sendStage.GetSentCount() == 0 {
		t.Error("Sending stage sent 0 events")
	}

	fmt.Printf("Pipeline test completed successfully!\n")
	fmt.Printf("Generated: %d, Sent: %d, Received: %d\n",
		genStage.GetGeneratedCount(), sendStage.GetSentCount(), len(receivedMessages))
}

func TestPipeline_HighThroughput(t *testing.T) {
	// Тест на высокую нагрузку
	server, received := startMockUDPServer(t, "127.0.0.1:0")
	defer server.Close()

	serverAddr := server.LocalAddr().String()

	// Создаем высокопроизводительный pipeline
	genStage := NewEventGenerationStage("high-load-gen", 100) // 100 events/sec
	sendStage := NewNetworkSendingStage("high-load-send")
	sendStage.SetDestinations([]string{serverAddr})
	sendStage.SetProtocol("udp")

	genAdapter := NewGenerationAdapter(genStage, 50) // Больший буфер
	sendAdapter := NewSendingAdapter(sendStage, 50)

	pipeline := coordinator.NewPipeline(50)
	pipeline.AddStage(genAdapter)
	pipeline.AddStage(sendAdapter)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := pipeline.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start high-throughput pipeline: %v", err)
	}

	// Считаем полученные сообщения
	messageCount := 0
	timeout := time.After(600 * time.Millisecond)

countLoop:
	for {
		select {
		case <-received:
			messageCount++
		case <-timeout:
			break countLoop
		}
	}

	pipeline.Stop()

	// При 100 events/sec за 0.5 секунды должно быть ~50 событий
	if messageCount < 30 {
		t.Errorf("Expected at least 30 messages at high throughput, got %d", messageCount)
	}

	fmt.Printf("High-throughput test: received %d messages\n", messageCount)
}
