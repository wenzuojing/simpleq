package main

import (
	"flag"
	"github.com/wenzuojing/simpleq/server"
	"log"
	"os"
	"os/signal"
)

var host string
var port int

func main() {

	flag.StringVar(&host, "host", "localhost", "-host localhost")
	flag.IntVar(&port, "port", 9090, "-port 9090")
	flag.Parse()
	log.SetPrefix("[simpleq]")

	go func() {
		err := server.StartServer(host, port)
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
