package clients

import (
	"github.com/pliu/kmon/pkg/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

// GetFranzGoClient returns a new franz-go kafka client
func GetFranzGoClient(cfg *config.KMonConfig) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaConfig.SeedBrokers...),
	}
	client, err := kgo.NewClient(opts...)
	return client, err
}
