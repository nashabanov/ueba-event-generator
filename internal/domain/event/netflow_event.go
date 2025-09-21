package event

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"time"
)

// NetflowEvent представляет Netflow v5 событие
type NetflowEvent struct {
	// Основные поля события
	ID             string    `json:"-"` // Внутренний ID
	EventTimestamp time.Time `json:"event_time"`
	LogTimestamp   time.Time `json:"logtime"`

	// Netflow специфичные поля
	InBytes         uint64     `json:"netflow_in_bytes"`      // Байты входящие
	InPackets       uint32     `json:"netflow_in_pkts"`       // Пакеты входящие
	SourceAddr      netip.Addr `json:"netflow_ipv4_src_addr"` // IP источника
	DestinationAddr netip.Addr `json:"netflow_ipv4_dst_addr"` // IP назначения
	SourcePort      uint16     `json:"netflow_l4_src_port"`   // Порт источника
	DestinationPort uint16     `json:"netflow_l4_dst_port"`   // Порт назначения
	Protocol        string     `json:"netflow_protocol"`      // TCP/UDP/ICMP
	TCPFlags        string     `json:"netflow_tcp_flags"`     // TCP флаги

	// Host информация
	HostIP        netip.Addr `json:"host_ip"`
	HostIPListStr string     `json:"host_ip_list_str"`

	// Observer информация
	ObserverType string `json:"observer_type"` // "netflow"

	// Кэшированный размер для производительности
	cachedSize int
}

func NewNetflowEvent() *NetflowEvent {
	now := time.Now().UTC()
	return &NetflowEvent{
		ID:             generateEventID(),
		EventTimestamp: now,
		LogTimestamp:   now,
		ObserverType:   "netflow",
		cachedSize:     -1,
	}
}

// Type возвращает тип события
func (e NetflowEvent) Type() EventType {
	return EventTypeNetflow
}

// Timestamp возвращает временную метку события
func (e NetflowEvent) Timestamp() time.Time {
	return e.EventTimestamp
}

// ID возвращает ID события
func (e NetflowEvent) GetID() string {
	return e.ID
}

// GetSourceIP возвращает IP-адрес источника
func (e NetflowEvent) GetSourceIP() netip.Addr {
	return e.SourceAddr
}

// GetDestinationIP возвращает IP-адрес назначения
func (e NetflowEvent) GetDestinationIP() netip.Addr {
	return e.DestinationAddr
}

// ToJSON преобразует событие в JSON
func (e NetflowEvent) ToJSON() ([]byte, error) {
	data := map[string]interface{}{
		"netflow_in_bytes":      e.InBytes,
		"netflow_in_pkts":       e.InPackets,
		"netflow_ipv4_src_addr": e.SourceAddr.String(),
		"netflow_ipv4_dst_addr": e.DestinationAddr.String(),
		"netflow_l4_src_port":   e.SourcePort,
		"netflow_l4_dst_port":   e.DestinationPort,
		"netflow_protocol":      e.Protocol,
		"netflow_tcp_flags":     e.TCPFlags,
		"logtime":               e.LogTimestamp.Format(time.RFC3339Nano),
		"host_ip":               e.HostIP.String(),
		"host_ip_list_str":      e.HostIPListStr,
		"event_time":            e.EventTimestamp.Format(time.RFC3339Nano),
		"observer_type":         e.ObserverType,
	}
	return json.Marshal(data)
}

// Size возвращает размер события в байтах
func (e NetflowEvent) Size() int {
	if e.cachedSize == -1 {
		jsonData, _ := e.ToJSON()
		e.cachedSize = len(jsonData)
	}
	return e.cachedSize
}

// Validate проверяет корректность Netflow события
func (e NetflowEvent) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("id is required")
	}

	if e.EventTimestamp.IsZero() {
		return fmt.Errorf("event_time is required")
	}

	if !e.SourceAddr.IsValid() {
		return fmt.Errorf("source address is invalid: %v", e.SourceAddr)
	}

	if !e.DestinationAddr.IsValid() {
		return fmt.Errorf("destination address is invalid: %v", e.DestinationAddr)
	}

	if e.Protocol == "" {
		return fmt.Errorf("protocol is required")
	}

	if e.ObserverType != "netflow" {
		return fmt.Errorf("observer type must be 'netflow', got: %v", e.ObserverType)
	}

	return nil
}

// SetTrafficData устанавливает данные трафика
func (e *NetflowEvent) SetTrafficData(srcIP, destIP string, srcPort, dstPort uint16, protocol string) error {
	var err error

	e.SourceAddr, err = netip.ParseAddr(srcIP)
	if err != nil {
		return fmt.Errorf("invalid source IP address: %v", err)
	}

	e.DestinationAddr, err = netip.ParseAddr(destIP)
	if err != nil {
		return fmt.Errorf("invalid destination IP address: %v", err)
	}

	e.SourcePort = srcPort
	e.DestinationPort = dstPort
	e.Protocol = protocol
	e.HostIP = e.SourceAddr
	e.HostIPListStr = e.SourceAddr.String()

	// Сбросить кэш размера
	e.cachedSize = -1

	return nil
}

// SetBytesAndPackets устанавливает статистику трафика
func (e *NetflowEvent) SetBytesAndPackets(bytes uint64, packets uint32) {
	e.InBytes = bytes
	e.InPackets = packets

	// Сбросить кэш размера
	e.cachedSize = -1
}
