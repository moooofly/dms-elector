package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	srv "github.com/moooofly/dms-elector/pkg/servitization"
)

func main() {
	if err := srv.Init(); err != nil {
		log.Fatalf("err : %s", err)
	}

	done := make(chan bool)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		defer func() {
			if e := recover(); e != nil {
				fmt.Printf("crashed, err: %s\nstack:\n%s", e, string(debug.Stack()))
			}
		}()
		for range signalChan {
			log.Println("Recv an Unix Signal, stopping...")
			srv.Teardown()
			done <- true
		}
	}()

	<-done

	os.Exit(0)
}
