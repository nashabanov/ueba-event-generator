package profiler

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

// StartProfiler запускает отдельный HTTP сервер для профилирования
func StartProfiler(port string) {
	if port == "" {
		port = "6060"
	}
	go func() {
		log.Printf("🔍 Pprof доступен: http://localhost:%s/debug/pprof/", port)

		// ListenAndServe блокирующий, поэтому запускаем в goroutine
		if err := http.ListenAndServe("localhost:"+port, nil); err != nil {
			log.Printf("❌ Ошибка pprof сервера: %v", err)
		}
	}()
}
