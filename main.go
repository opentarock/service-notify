package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"

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

	handlers := service.NewNotifyServiceHandlers()
	notifyService.AddHeaderHandler(proto_notify.MessageUsersHeaderMessage, handlers.MessageUsersHandler())

	err := notifyService.Start()
	if err != nil {
		log.Fatalf("Error starting lobby service: %s", err)
	}
	defer notifyService.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	sig := <-c
	log.Printf("Interrupted by %s", sig)
}
