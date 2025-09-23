package stages

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ------------------- NetworkSendingStage -------------------

func TestNewNetworkSendingStage(t *testing.T) {
	stage := NewNetworkSendingStage("test_stage")
	require.Equal(t, "test_stage", stage.Name())
	require.Equal(t, "udp", stage.protocol)
	require.Len(t, stage.destinations, 1)
	require.NotNil(t, stage.workerPool)
	require.NotNil(t, stage.metrics)
	require.NotNil(t, stage.input)
}

func TestSetDestinations(t *testing.T) {
	stage := NewNetworkSendingStage("test_stage")

	// Валидные адреса → нет ошибки
	err := stage.SetDestinations([]string{"127.0.0.1:514", "192.168.0.1:514"})
	require.NoError(t, err)
	require.Len(t, stage.destinations, 2)

	// Пустой список → ошибка
	err = stage.SetDestinations([]string{})
	require.Error(t, err)

	// Некорректный адрес → ошибка
	err = stage.SetDestinations([]string{"127.0.0.1"}) // нет порта
	require.Error(t, err)
}

func TestSetProtocol(t *testing.T) {
	stage := NewNetworkSendingStage("test_stage")

	// Валидные протоколы
	require.NoError(t, stage.SetProtocol("tcp"))
	require.Equal(t, "tcp", stage.protocol)

	require.NoError(t, stage.SetProtocol("udp"))
	require.Equal(t, "udp", stage.protocol)

	// Невалидный протокол
	err := stage.SetProtocol("icmp")
	require.Error(t, err)
}

func TestSendData_UDP_Error(t *testing.T) {
	stage := NewNetworkSendingStage("udp_stage")
	data := &SerializedData{
		Data:        []byte("abc"),
		EventID:     "evt_1",
		Protocol:    "udp",
		Size:        3,
		Destination: "256.256.256.256:123", // гарантированно недоступный адрес
	}
	err := stage.SendData(data)
	require.Error(t, err)
}

func TestSendData_TCP_Error(t *testing.T) {
	stage := NewNetworkSendingStage("tcp_stage")
	stage.protocol = "tcp"
	data := &SerializedData{
		Data:        []byte("abc"),
		EventID:     "evt_1",
		Protocol:    "tcp",
		Size:        3,
		Destination: "127.0.0.1:514",
	}
	// tcpPool не инициализирован → ошибка
	err := stage.SendData(data)
	require.Error(t, err)
}

func TestSendData_UnsupportedProtocol(t *testing.T) {
	stage := NewNetworkSendingStage("stage")
	data := &SerializedData{
		Data:     []byte("abc"),
		EventID:  "evt_1",
		Protocol: "icmp",
		Size:     3,
	}
	err := stage.SendData(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported protocol")
}

func TestNetworkSendJob_Execute_Error(t *testing.T) {
	stage := NewNetworkSendingStage("job_stage")
	data := &SerializedData{
		Data:        []byte("abc"),
		EventID:     "evt_1",
		Protocol:    "udp",
		Size:        3,
		Destination: "256.256.256.256:123",
	}
	job := &NetworkSendJob{stage: stage, data: data}
	err := job.Execute()
	require.Error(t, err)
}

func TestStage_MetricsMethods_Types(t *testing.T) {
	stage := NewNetworkSendingStage("metrics_stage")

	// Проверяем только типы/ключи
	require.IsType(t, uint64(0), stage.GetSentCount())
	require.IsType(t, uint64(0), stage.GetFailedCount())

	stats := stage.GetStageStats()
	require.Equal(t, "metrics_stage", stats["stage_name"])
	require.Equal(t, "udp", stats["protocol"])
	require.True(t, stats["worker_pool_healthy"].(bool))
}

func TestIsHealthy(t *testing.T) {
	stage := NewNetworkSendingStage("healthy_stage")
	healthy, msg := stage.IsHealthy()
	require.True(t, healthy)
	require.Equal(t, "stage healthy", msg)
}

func TestOptimizationRecommendations_Format(t *testing.T) {
	stage := NewNetworkSendingStage("opt_stage")
	rec := stage.GetOptimizationRecommendations()
	require.IsType(t, "", rec[0])
}

func TestRun_ContextCancel(t *testing.T) {
	stage := NewNetworkSendingStage("run_stage")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	inCh := make(chan *SerializedData)
	err := stage.Run(ctx, inCh)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRun_ProcessData_Error(t *testing.T) {
	stage := NewNetworkSendingStage("run_process")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	inCh := make(chan *SerializedData, 1)
	inCh <- &SerializedData{
		Data:        []byte("abc"),
		EventID:     "evt_1",
		Protocol:    "udp",
		Size:        3,
		Destination: "256.256.256.256:123",
	}

	err := stage.Run(ctx, inCh)
	require.Error(t, err)
}
