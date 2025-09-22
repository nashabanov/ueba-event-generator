package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

type TCPTestServer struct {
	port            string
	listener        net.Listener
	receivedCount   uint64
	bytesReceived   uint64
	connectionCount uint64
	startTime       time.Time
}

func NewTCPTestServer(port string) *TCPTestServer {
	return &TCPTestServer{
		port:      port,
		startTime: time.Now(),
	}
}

func (s *TCPTestServer) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", "0.0.0.0:"+s.port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", s.port, err)
	}

	log.Printf("🚀 TCP Test Server listening on :%s", s.port)

	// Статистика каждые 5 секунд
	go s.printStats()

	// Принимаем соединения
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Printf("❌ Accept error: %v", err)
			continue
		}

		atomic.AddUint64(&s.connectionCount, 1)
		go s.handleConnection(conn)
	}
}

func (s *TCPTestServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	connID := atomic.LoadUint64(&s.connectionCount)
	log.Printf("📡 Connection %d from %s", connID, conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	for {
		// Читаем данные (ожидаем JSON события)
		data, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("❌ Read error from %s: %v", conn.RemoteAddr(), err)
			}
			break
		}

		atomic.AddUint64(&s.receivedCount, 1)
		atomic.AddUint64(&s.bytesReceived, uint64(len(data)))

		// Опционально: парсим JSON для валидации
		// var event map[string]interface{}
		// json.Unmarshal(data, &event)
	}

	log.Printf("📡 Connection %d closed", connID)
}

func (s *TCPTestServer) printStats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastCount uint64
	var lastTime time.Time = time.Now()

	for range ticker.C {
		currentCount := atomic.LoadUint64(&s.receivedCount)
		currentBytes := atomic.LoadUint64(&s.bytesReceived)
		connections := atomic.LoadUint64(&s.connectionCount)

		now := time.Now()
		duration := now.Sub(lastTime).Seconds()
		eps := float64(currentCount-lastCount) / duration

		totalDuration := now.Sub(s.startTime).Seconds()
		avgEPS := float64(currentCount) / totalDuration

		log.Printf("📊 Stats: Events=%d (%.1f EPS, avg %.1f), Bytes=%d KB, Connections=%d",
			currentCount, eps, avgEPS, currentBytes/1024, connections)

		lastCount = currentCount
		lastTime = now
	}
}

func (s *TCPTestServer) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func main() {
	port := "514"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	server := NewTCPTestServer(port)

	// Graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		log.Printf("🛑 Shutting down server...")
		server.Stop()
		os.Exit(0)
	}()

	if err := server.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
