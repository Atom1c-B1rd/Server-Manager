package main

import (
	"log"
	"os/exec"
	"runtime"
	"time"

	managers "server-manager/Managers"
	ui "server-manager/UI"
)

func main() {
	log.Println("Iniciando Go Server Manager...")

	managers.GlobalManager.AutoStartAll()

	go func() {
		time.Sleep(500 * time.Millisecond)

		url := "http://localhost:3000/"

		var cmd *exec.Cmd

		switch runtime.GOOS {
		case "windows":
			cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
		case "darwin":
			cmd = exec.Command("open", url)
		default:
			cmd = exec.Command("xdg-open", url)
		}

		if err := cmd.Start(); err != nil {
			log.Printf("No se pudo abrir el navegador automáticamente: %v", err)
		}
	}()

	ui.UI()
}
