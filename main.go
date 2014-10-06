package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"

	"github.com/opentarock/service-api/go/client"
	nservice "github.com/opentarock/service-api/go/service"

	"github.com/opentarock/service-api/go/proto_notify"
	"github.com/opentarock/service-notify/service"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	// profiliing related flag
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	log.SetFlags(log.Ldate | log.Lmicroseconds)

	notifyService := nservice.NewRepService("tcp://*:8001")

	presenceClient := client.NewPresenceClientNanomsg()
	err := presenceClient.Connect("tcp://localhost:9001")
	if err != nil {
		log.Fatalf("Failed to connect to presence service: ", err)
	}
	defer presenceClient.Close()

	gcmClient := client.NewGcmClientNanomsg()
	err = gcmClient.Connect("tcp://localhost:11001")
	if err != nil {
		log.Fatalf("Failed to connect to gcm service: ", err)
	}
	defer gcmClient.Close()

	handlers := service.NewNotifyServiceHandlers(presenceClient, gcmClient)
	notifyService.AddHeaderHandler(proto_notify.MessageUsersHeaderMessage, handlers.MessageUsersHandler())

	err = notifyService.Start()
	if err != nil {
		log.Fatalf("Error starting notify service: %s", err)
	}
	defer notifyService.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	sig := <-c
	log.Printf("Interrupted by %s", sig)
}
