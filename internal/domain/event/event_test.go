package event

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNetflowEvent_Creation(t *testing.T) {
	event := NewNetflowEvent()

	if event.Type() != EventTypeNetflow {
		t.Errorf("Expected event type %v, got %v", EventTypeNetflow, event.Type())
	}

	if event.ObserverType != "netflow" {
		t.Errorf("Expected observer_type 'netflow', got %s", event.ObserverType)
	}

	if event.GetID() == "" {
		t.Error("Event ID should not be empty")
	}

	if time.Since(event.Timestamp()) > time.Second {
		t.Error("Event timestamp should be recent")
	}
}

func TestNetflowEvent_SetTrafficData(t *testing.T) {
	event := NewNetflowEvent()

	err := event.SetTrafficData("192.168.1.100", "203.0.113.84", 54231, 443, "TCP")
	if err != nil {
		t.Fatalf("SetTrafficData failed: %v", err)
	}

	if event.SourceAddr.String() != "192.168.1.100" {
		t.Errorf("Expected source IP 192.168.1.100, got %s", event.SourceAddr.String())
	}

	if event.DestinationAddr.String() != "203.0.113.84" {
		t.Errorf("Expected destination IP 203.0.113.84, got %s", event.DestinationAddr.String())
	}

	if event.SourcePort != 54231 {
		t.Errorf("Expected source port 54231, got %d", event.SourcePort)
	}

	if event.DestinationPort != 443 {
		t.Errorf("Expected destination port 443, got %d", event.DestinationPort)
	}

	if event.Protocol != "TCP" {
		t.Errorf("Expected protocol TCP, got %s", event.Protocol)
	}
}

func TestNetflowEvent_JSON_Serialization(t *testing.T) {
	event := NewNetflowEvent()
	event.SetTrafficData("192.168.1.100", "203.0.113.84", 54231, 443, "TCP")
	event.SetBytesAndPackets(745200, 498)
	event.TCPFlags = "SA"

	jsonData, err := event.ToJSON()
	if err != nil {
		t.Fatalf("JSON serialization failed: %v", err)
	}

	// Проверяем что JSON содержит ожидаемые поля
	var parsed map[string]interface{}
	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		t.Fatalf("JSON parsing failed: %v", err)
	}

	if parsed["netflow_ipv4_src_addr"] != "192.168.1.100" {
		t.Errorf("Expected source IP in JSON, got %v", parsed["netflow_ipv4_src_addr"])
	}

	if parsed["netflow_in_bytes"].(float64) != 745200 {
		t.Errorf("Expected bytes 745200 in JSON, got %v", parsed["netflow_in_bytes"])
	}

	if parsed["observer_type"] != "netflow" {
		t.Errorf("Expected observer_type 'netflow' in JSON, got %v", parsed["observer_type"])
	}
}

func TestSyslogEvent_Creation(t *testing.T) {
	event := NewSyslogEvent()

	if event.Type() != EventTypeSyslog {
		t.Errorf("Expected event type %v, got %v", EventTypeSyslog, event.Type())
	}

	if event.DeviceVendor != "PaloAlto" {
		t.Errorf("Expected device vendor 'PaloAlto', got %s", event.DeviceVendor)
	}

	if event.DeviceProduct != "PAN-OS" {
		t.Errorf("Expected device product 'PAN-OS', got %s", event.DeviceProduct)
	}
}

func TestSyslogEvent_SetNetworkData(t *testing.T) {
	event := NewSyslogEvent()

	err := event.SetNetworkData("192.168.142.87", "203.0.113.84", 54231, "tcp")
	if err != nil {
		t.Fatalf("SetNetworkData failed: %v", err)
	}

	if event.SourceAddress.String() != "192.168.142.87" {
		t.Errorf("Expected source IP 192.168.142.87, got %s", event.SourceAddress.String())
	}

	expectedMsg := "PaloAlto TRAFFIC src=192.168.142.87 dst=203.0.113.84 dport=54231"
	if event.EventMessage != expectedMsg {
		t.Errorf("Expected message '%s', got '%s'", expectedMsg, event.EventMessage)
	}
}

func TestEventFactory(t *testing.T) {
	factory := NewEventFactory()

	// Test Netflow creation
	netflowEvent, err := factory.CreateEvent(EventTypeNetflow)
	if err != nil {
		t.Fatalf("Failed to create Netflow event: %v", err)
	}

	if netflowEvent.Type() != EventTypeNetflow {
		t.Errorf("Expected Netflow event type, got %v", netflowEvent.Type())
	}

	// Test Syslog creation
	syslogEvent, err := factory.CreateEvent(EventTypeSyslog)
	if err != nil {
		t.Fatalf("Failed to create Syslog event: %v", err)
	}

	if syslogEvent.Type() != EventTypeSyslog {
		t.Errorf("Expected Syslog event type, got %v", syslogEvent.Type())
	}

	// Test unknown type
	_, err = factory.CreateEvent(EventType(99))
	if err == nil {
		t.Error("Expected error for unknown event type")
	}
}

func TestEvent_Size_Calculation(t *testing.T) {
	event := NewNetflowEvent()
	event.SetTrafficData("192.168.1.100", "203.0.113.84", 54231, 443, "TCP")

	size1 := event.Size()
	size2 := event.Size()

	// Размер должен кэшироваться
	if size1 != size2 {
		t.Error("Event size should be cached")
	}

	if size1 <= 0 {
		t.Errorf("Event size should be positive, got %d", size1)
	}

	// После изменения размер должен пересчитываться
	event.SetBytesAndPackets(1000000, 1000)
	size3 := event.Size()

	if size3 == size1 {
		t.Error("Event size should change after modification")
	}
}

func BenchmarkNetflowEvent_ToJSON(b *testing.B) {
	event := NewNetflowEvent()
	event.SetTrafficData("192.168.1.100", "203.0.113.84", 54231, 443, "TCP")
	event.SetBytesAndPackets(745200, 498)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := event.ToJSON()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSyslogEvent_ToJSON(b *testing.B) {
	event := NewSyslogEvent()
	event.SetNetworkData("192.168.142.87", "203.0.113.84", 54231, "tcp")
	event.SetTrafficStats(4821, 745200, 1254300)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := event.ToJSON()
		if err != nil {
			b.Fatal(err)
		}
	}
}
