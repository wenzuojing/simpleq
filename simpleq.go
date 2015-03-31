package main

import (
	"flag"
	"github.com/wenzuojing/simpleq/config"
	"github.com/wenzuojing/simpleq/server"
	"log"
	"os"
	"os/signal"
)

func main() {

	log.Println(os.Args[0])

	var configPath string
	flag.StringVar(&configPath, "conf", "simpleq.conf", "-conf simpleq.conf")
	flag.Parse()
	log.SetPrefix("[simpleq]")
	log.SetFlags(log.Lshortfile)

	config, err := config.LoadConfig(configPath)

	if err != nil {
		log.Fatalf("Read config fail. %v", err)
	}

	if logFile, found := config.String("log.file"); found {
		if file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666); err == nil {
			defer file.Close()
			log.SetOutput(file)
		}

	}

	go func() {
		err := server.StartServer(config)
		if err != nil {
			log.Fatalf("[error] start server fail.\n%v\n", err)
		}

	}()

	//Kill the app on signal.
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, os.Kill)
	<-ch
	os.Exit(1)

}
