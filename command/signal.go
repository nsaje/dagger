package command

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func handleSignals() {
	signalCh := make(chan os.Signal, 4)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	// Wait for a signal
	// TODO: add finer-grained handling
	for {
		select {
		case s := <-signalCh:
			log.Printf("Caught signal: %v", s)
			// panic("stack")
			return
		}
	}
}
