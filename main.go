package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/TFMV/flashfs/cmd"
)

func main() {
	// Set up logging
	log.SetPrefix("flashfs: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	// Create a context that will be canceled on interrupt
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Handle signals in a separate goroutine
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v", sig)
		cancel()

		// Force exit after 5 seconds if graceful shutdown fails
		time.AfterFunc(5*time.Second, func() {
			log.Printf("Forcing exit after timeout")
			os.Exit(1)
		})
	}()

	// Recover from panics
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic: %v\n", r)
			log.Printf("Stack trace:\n%s", debug.Stack())
			os.Exit(1)
		}
	}()

	// Execute the command with the cancellable context
	if err := cmd.ExecuteWithContext(ctx); err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}
