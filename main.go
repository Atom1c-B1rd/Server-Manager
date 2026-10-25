package main

import (
	"log"

	"server-manager/Managers"
	"server-manager/UI"
)

func main() {
	log.Println("Iniciando Go Server Manager...")
	managers.GlobalManager.AutoStartAll()
	ui.UI()
}
