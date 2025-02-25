package main

import (
	"github.com/youjiaxing/sd"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ds, err := sd.NewDiscover([]string{"127.0.0.1:2379"})
	if err != nil {
		log.Fatalln(err)
	}

	ds.Watch("vk-server")
	ds.Watch("test")
	ds.Start()

	go func() {
		for {
			log.Printf("vk-service addrs: %v\n", ds.QueryService("vk-server"))
			log.Printf("test addrs: %v\n", ds.QueryService("test"))
			time.Sleep(time.Second)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)
	<-sig
	ds.Stop()
}
