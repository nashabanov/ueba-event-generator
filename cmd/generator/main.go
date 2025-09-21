package main

import (
    "flag"
    "fmt"
)

// Версионная информация (заполняется при сборке)
var (
    version   = "dev"           // Версия приложения
    buildTime = "unknown"       // Время сборки
    gitCommit = "unknown"       // Git коммит
)

func main() {
    // Парсим самые базовые флаги
    var showVersion bool
    var showHelp bool
    
    flag.BoolVar(&showVersion, "version", false, "Show version information")
    flag.BoolVar(&showVersion, "v", false, "Show version (shorthand)")
    
    flag.BoolVar(&showHelp, "help", false, "Show help information")
    flag.BoolVar(&showHelp, "h", false, "Show help (shorthand)")
    
    flag.Parse()
    
    // Обрабатываем специальные флаги
    if showVersion {
        showVersionInfo()
        return
    }
    
    if showHelp {
        showHelpInfo()
        return
    }
    
    // Пока что просто выводим сообщение
    fmt.Println("UEBA Event Generator starting...")
    fmt.Println("This is just the beginning! More features coming...")
}

// showVersionInfo выводит информацию о версии
func showVersionInfo() {
    fmt.Printf("UEBA Event Generator\n")
    fmt.Printf("Version: %s\n", version)
    fmt.Printf("Build Time: %s\n", buildTime)
    fmt.Printf("Git Commit: %s\n", gitCommit)
}

// showHelpInfo выводит базовую справку
func showHelpInfo() {
    fmt.Printf("UEBA Event Generator - Generate synthetic security events\n\n")
    fmt.Printf("USAGE:\n")
    fmt.Printf("  ueba-generator [FLAGS]\n\n")
    fmt.Printf("FLAGS:\n")
    fmt.Printf("  -v, --version            Show version information\n")
    fmt.Printf("  -h, --help               Show this help\n\n")
    fmt.Printf("More options will be added in next steps!\n")
}
