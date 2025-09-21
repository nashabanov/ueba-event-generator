package event

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"time"
)

// SyslogEvent представляет Syslog событие (PaloAlto format)
type SyslogEvent struct {
	// Основные поля события
	ID             string    `json:"-"`       // Внутренний ID
	EventTimestamp time.Time `json:"-"`       // Внутренний timestamp
	LogTimestamp   time.Time `json:"logtime"` // "2025-04-05 14:30:22"

	// Device информация
	DeviceProduct    string `json:"deviceProduct"`    // "PAN-OS"
	DeviceVendor     string `json:"deviceVendor"`     // "PaloAlto"
	DeviceVersion    string `json:"deviceVersion"`    // "10.1.0"
	DeviceExternalID string `json:"deviceExternalId"` // "serial-7429"
	DeviceHostName   string `json:"deviceHostName"`   // "palo-4271"
	Host             string `json:"host"`             // "palo-4271"

	// Network информация
	SourceAddress       netip.Addr `json:"sourceAddress"`
	DestinationAddress  netip.Addr `json:"destinationAddress"`
	DestinationPort     uint16     `json:"destinationPort"`
	TransportProtocol   string     `json:"transportProtocol"`   // "tcp"
	ApplicationProtocol string     `json:"applicationProtocol"` // "app-15"

	// Event классификация
	Name                string `json:"name"`                // "TRAFFIC"
	EventCategory       string `json:"event_category"`      // "network"
	EventType           string `json:"event_type"`          // "allowed"
	EventAction         string `json:"event_action"`        // "allowed"
	DeviceEventCategory string `json:"deviceEventCategory"` // "TRAFFIC"

	// Traffic статистика
	Packets    uint32 `json:"Packets,string"`     // "4821"
	BytesIn    uint64 `json:"bytesIn,string"`     // "745200"
	BytesOut   uint64 `json:"bytesOut,string"`    // "1254300"
	TotalBytes uint64 `json:"Total_bytes,string"` // "1999500"

	// Syslog специфичные поля
	SyslogTimestamp string `json:"syslog_timestamp"` // "Apr 5 14:30:22"
	SyslogHostname  string `json:"syslog_hostname"`  // "palo-4271"

	// Security поля
	SessionID       string `json:"SessionID"`        // "8473921"
	SourceZone      string `json:"Source_Zone"`      // "trust"
	DestinationZone string `json:"Destination_Zone"` // "untrust"
	URLCategory     string `json:"URL_Category"`
	Flags           string `json:"Flags"`

	// Сообщения
	EventMessage string `json:"event_message"`
	RawDataField string `json:"raw_data_field"` // Полное сырое сообщение

	// Кэшированный размер
	cachedSize int
}

// Timestamp implements Event.
func (e *SyslogEvent) Timestamp() time.Time {
	panic("unimplemented")
}

// NewSyslogEvent создает новое Syslog событие
func NewSyslogEvent() *SyslogEvent {
	now := time.Now()
	return &SyslogEvent{
		ID:             generateEventID(),
		EventTimestamp: now,
		LogTimestamp:   now,
		DeviceProduct:  "PAN-OS",
		DeviceVendor:   "PaloAlto",
		EventCategory:  "network",
		cachedSize:     -1,
	}
}

// Type возвращает тип события
func (e *SyslogEvent) Type() EventType {
	return EventTypeSyslog
}

// Timestamp возвращает время события
func (e *SyslogEvent) GetTimestamp() time.Time {
	return e.EventTimestamp
}

// ID возвращает уникальный идентификатор события
func (e *SyslogEvent) GetID() string {
	return e.ID
}

// GetSourceIP возвращает IP источника
func (e *SyslogEvent) GetSourceIP() netip.Addr {
	return e.SourceAddress
}

// GetDestinationIP возвращает IP назначения
func (e *SyslogEvent) GetDestinationIP() netip.Addr {
	return e.DestinationAddress
}

// ToJSON сериализует событие в JSON
func (e *SyslogEvent) ToJSON() ([]byte, error) {
	// Создаем копию структуры для кастомной сериализации
	data := struct {
		*SyslogEvent
		SourceAddress      string `json:"sourceAddress"`
		DestinationAddress string `json:"destinationAddress"`
		LogTime            string `json:"logtime"`
	}{
		SyslogEvent:        e,
		SourceAddress:      e.SourceAddress.String(),
		DestinationAddress: e.DestinationAddress.String(),
		LogTime:            e.LogTimestamp.Format("2006-01-02 15:04:05"), // Custom format
	}

	return json.Marshal(data)
}

// Size вычисляет размер события в байтах
func (e *SyslogEvent) Size() int {
	if e.cachedSize == -1 {
		jsonData, _ := e.ToJSON()
		e.cachedSize = len(jsonData)
	}
	return e.cachedSize
}

// Validate проверяет корректность Syslog события
func (e *SyslogEvent) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("event ID cannot be empty")
	}

	if e.EventTimestamp.IsZero() {
		return fmt.Errorf("event timestamp cannot be zero")
	}

	if !e.SourceAddress.IsValid() {
		return fmt.Errorf("source address is invalid: %v", e.SourceAddress)
	}

	if !e.DestinationAddress.IsValid() {
		return fmt.Errorf("destination address is invalid: %v", e.DestinationAddress)
	}

	if e.DeviceVendor == "" {
		return fmt.Errorf("device vendor cannot be empty")
	}

	if e.DeviceProduct == "" {
		return fmt.Errorf("device product cannot be empty")
	}

	return nil
}

// SetNetworkData устанавливает сетевые данные события
func (e *SyslogEvent) SetNetworkData(srcIP, dstIP string, dstPort uint16, protocol string) error {
	var err error

	e.SourceAddress, err = netip.ParseAddr(srcIP)
	if err != nil {
		return fmt.Errorf("invalid source IP %s: %w", srcIP, err)
	}

	e.DestinationAddress, err = netip.ParseAddr(dstIP)
	if err != nil {
		return fmt.Errorf("invalid destination IP %s: %w", dstIP, err)
	}

	e.DestinationPort = dstPort
	e.TransportProtocol = protocol

	// Генерируем сообщение
	e.EventMessage = fmt.Sprintf("PaloAlto TRAFFIC src=%s dst=%s dport=%d",
		srcIP, dstIP, dstPort)

	// Сбросить кэш размера
	e.cachedSize = -1

	return nil
}

// SetTrafficStats устанавливает статистику трафика
func (e *SyslogEvent) SetTrafficStats(packets uint32, bytesIn, bytesOut uint64) {
	e.Packets = packets
	e.BytesIn = bytesIn
	e.BytesOut = bytesOut
	e.TotalBytes = bytesIn + bytesOut
	e.cachedSize = -1
}

// SetDeviceInfo устанавливает информацию об устройстве
func (e *SyslogEvent) SetDeviceInfo(hostname, version, serialID string) {
	e.DeviceHostName = hostname
	e.Host = hostname
	e.SyslogHostname = hostname
	e.DeviceVersion = version
	e.DeviceExternalID = serialID
	e.cachedSize = -1
}
