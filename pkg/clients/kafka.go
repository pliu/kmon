package clients

import (
	"context"

	"github.com/pliu/kmon/pkg/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KgoClient is an interface for the Kafka client
type KgoClient interface {
	Close()
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
	PollFetches(context.Context) kgo.Fetches
}

// GetFranzGoClient returns a new franz-go kafka client
func GetFranzGoClient(cfg *config.KafkaConfig) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.SeedBrokers...),
	}
	client, err := kgo.NewClient(opts...)
	return client, err
}
