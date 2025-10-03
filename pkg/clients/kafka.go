package clients

import (
	"context"

	"github.com/pliu/kmon/pkg/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KgoClient is an interface for the Kafka client
type KgoClient interface {
	Close()
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	PollFetches(context.Context) KgoFetches
}

// KgoFetches is an interface for the Kafka fetches
type KgoFetches interface {
	IsClientClosed() bool
	EachError(func(string, int32, error))
	EachRecord(func(*kgo.Record))
	Err() error
}

// KadmClient is an interface for the Kafka admin client
type KadmClient interface {
	Close()
	ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
}

// KgoClientWrapper wraps a *kgo.Client to implement the KgoClient interface
type KgoClientWrapper struct {
	*kgo.Client
}

func (w *KgoClientWrapper) PollFetches(ctx context.Context) KgoFetches {
	return &KgoFetchesWrapper{w.Client.PollFetches(ctx)}
}

// KgoFetchesWrapper wraps a kgo.Fetches to implement the KgoFetches interface
type KgoFetchesWrapper struct {
	kgo.Fetches
}

// GetFranzGoClient returns a new franz-go kafka client
func GetFranzGoClient(cfg *config.KafkaConfig) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.SeedBrokers...),
	}
	client, err := kgo.NewClient(opts...)
	return client, err
}
