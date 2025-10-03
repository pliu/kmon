package kmon

import (
	"context"

	"github.com/pliu/kmon/pkg/clients"
	"github.com/pliu/kmon/pkg/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KMon struct {
	config           *config.KMonConfig
	kafkaClient      *kgo.Client
	kafkaAdminClient *kadm.Client
	TopicManager     *TopicManager
}

func (k *KMon) Start(ctx context.Context) {
}

func NewKMon(cfg *config.KMonConfig) (*KMon, error) {
	client, err := clients.GetFranzGoClient(cfg)
	if err != nil {
		return nil, err
	}
	adminClient := kadm.NewClient(client)
	return &KMon{
		config:           cfg,
		kafkaClient:      client,
		kafkaAdminClient: adminClient,
		TopicManager:     NewTopicManager(*cfg, adminClient),
	}, nil
}
