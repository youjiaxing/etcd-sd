package main

import (
	"github.com/youjiaxing/sd"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	register, err := sd.NewRegister([]string{"127.0.0.1:2379"})
	if err != nil {
		log.Fatalln(err)
	}

	port := rand.Intn(10000)
	register.Reg("vk-server", "127.0.0.1:"+strconv.Itoa(port))
	err = register.Start()
	if err != nil {
		log.Fatal(err)
	}

	// 等待 term 信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	register.Stop()
}
