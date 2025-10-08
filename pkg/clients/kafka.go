package clients

import (
	"context"

	"github.com/pliu/kmon/pkg/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KgoClient interface {
	Close()
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
	PollFetches(context.Context) kgo.Fetches
}

func GetFranzGoClient(cfg *config.KafkaConfig, consumeTopics ...string) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.SeedBrokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	}

	if len(consumeTopics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(consumeTopics...))
	}

	return kgo.NewClient(opts...)
}
