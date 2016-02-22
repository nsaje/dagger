package command

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/Sirupsen/logrus"
)

func handleSignals(errc chan error) {
	signalCh := make(chan os.Signal, 4)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	// Wait for a signal
	// TODO: add finer-grained handling
	for {
		select {
		case err := <-errc:
			log.Printf("[ERROR] %s", err.Error())
			return
		case s := <-signalCh:
			log.Printf("Caught signal: %v", s)
			// panic("stack")
			return
		}
	}
}
