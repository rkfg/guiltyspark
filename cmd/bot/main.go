package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/rkfg/guiltyspark/bot"
	"github.com/rkfg/guiltyspark/config"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	doReembed := flag.Bool("reembed", false, "Re-embed all documents in the index (no Matrix client)")
	reembedIndex := flag.String("reembed.index", "", "Bleve index path (default: <storage_path>/index.bleve)")
	flag.Parse()
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	if *doReembed {
		cfg, err := config.LoadForReembed(*configPath)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}

		indexPath := cfg.StoragePath + "/index.bleve"
		if *reembedIndex != "" {
			indexPath = *reembedIndex
		}

		err = reembed(indexPath, cfg)
		if err != nil {
			log.Fatalf("Reembed failed: %v", err)
		}
		fmt.Println("Reembed completed.")
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	b, err := bot.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create bot: %v", err)
	}

	if err := b.Start(); err != nil {
		log.Fatalf("Failed to start bot: %v", err)
	}

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("Received signal: %v, shutting down...", sig)

	b.Stop()
	fmt.Println("Bot stopped.")
}
