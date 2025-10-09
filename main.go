package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/phuslu/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pliu/kmon/pkg/config"
	"github.com/pliu/kmon/pkg/kmon"
)

var (
	debug       = flag.Bool("debug", false, "Enable debug logging")
	metricsPort = flag.Int("metrics.port", 2112, "Port for the Prometheus metrics server")
	configPath  = flag.String("config.path", "config.yaml", "Path to the configuration file")
)

func main() {
	flag.Parse()

	log.DefaultLogger = log.Logger{
		Caller:     1,
		TimeFormat: "2006-01-02 15:04:05",
	}

	if *debug {
		log.DefaultLogger.Level = log.DebugLevel
		log.Debug().Msg("Debug logging enabled")
	}

	fmt.Printf("Using config file: %s\n", *configPath)
	// TODO: Implement configuration loading from *configPath

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		log.Info().Msg("Shutdown signal received")
		cancel()
	}()

	k, err := kmon.NewKMonFromConfig(&config.KMonConfig{}, ctx)
	if err != nil {
		// TODO log inside
		log.Fatal().Err(err).Msg("failed to create monitor instance")
	}
	go k.Start()

	// Setup Prometheus metrics server
	addr := fmt.Sprintf(":%d", *metricsPort)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Info().Msgf("Starting Prometheus metrics server on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Prometheus metrics server failed")
		}
	}()

	log.Info().Msg("kmon started")
	<-ctx.Done()

	log.Info().Msg("Shutting down server...")

	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal().Err(err).Msg("Server shutdown failed")
	}

	log.Info().Msg("kmon stopped")
}
