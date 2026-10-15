package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	topic := getenv("KAFKA_TOPIC", "maxwell")
	group := getenv("KAFKA_GROUP", "maxwell-printer")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     strings.Split(brokers, ","),
		Topic:       topic,
		GroupID:     group,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		// The printer often starts before Maxwell creates the topic. Without
		// this, the group joins, gets zero partitions, and never reads.
		WatchPartitionChanges: true,
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("consuming topic %q from %s (group %s)", topic, brokers, group)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("stopped")
				return
			}
			log.Fatalf("read: %v", err)
		}
		fmt.Printf("partition=%d offset=%d key=%s\n%s\n\n", msg.Partition, msg.Offset, msg.Key, msg.Value)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
