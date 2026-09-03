package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ohait/gossip2/enc"
	lib "github.com/ohait/gossip2/lib"
	gossip "github.com/ohait/gossip2/net"
)

func main() {
	listenAddr := flag.String("l", "", "listen address (host:port, :port, or host)")
	flag.Parse()

	args := flag.Args()

	// folder is required
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: gossip2 [-l addr] folder")
		os.Exit(1)
	}
	folder := args[0]

	// Parse listen address with defaults: host=localhost, port=1337
	addr := *listenAddr
	var host, port string
	if addr != "" {
		if strings.Contains(addr, ":") {
			parts := strings.SplitN(addr, ":", 2)
			host = parts[0]
			port = parts[1]
			if port == "" {
				port = "1337"
			}
		} else {
			host = addr
			port = "1337"
		}
	} else {
		host = "localhost"
		port = "1337"
	}

	// Validate port
	if _, err := strconv.Atoi(port); err != nil {
		log.Fatalf("invalid port: %v", err)
	}

	listenAddrParsed := net.JoinHostPort(host, port)

	// Create the client (which manages logs)
	client, err := lib.New(folder)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	// Create and start the server
	server := &gossip.Server{
		Client: client,
	}

	enc.LOG("starting server on %s", listenAddrParsed)
	if err := server.Listen(listenAddrParsed); err != nil {
		log.Fatalf("server error: %v", err)
	}

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	enc.LOG("press Ctrl+C to quit")
	<-quit
	enc.LOG("shutting down...")
	server.Shutdown()
	time.Sleep(500 * time.Millisecond) // give some time, but we need graceful shutdown later
}
