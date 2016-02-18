package command

import (
	log "github.com/Sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
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
