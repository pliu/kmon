package kmon

import (
	"context"

	"github.com/phuslu/log"
	"github.com/pliu/kmon/pkg/config"
)

type KMon struct {
	monitor           *Monitor
	topicManager      *TopicManager
	cfg               *config.KMonConfig
	rootCtx           context.Context
	monitorCancelFunc context.CancelFunc
}

func NewKMonFromConfig(cfg *config.KMonConfig, ctx context.Context) (*KMon, error) {
	topicManager, err := NewTopicManagerFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &KMon{
		topicManager: topicManager,
		cfg:          cfg,
		rootCtx:      ctx,
	}, nil
}

func (k *KMon) Start() {
	k.topicManager.changeDetectedCallback = k.changeDetectedCallback
	k.topicManager.doneReconcilingCallback = k.doneReconcilingCallback

	k.topicManager.Start(k.rootCtx)
}

func (k *KMon) changeDetectedCallback() {
	if k.monitorCancelFunc != nil {
		k.monitorCancelFunc()
	}
}

func (k *KMon) doneReconcilingCallback(numPartitions int) {
	var partitions []int32
	for i := range numPartitions {
		partitions = append(partitions, int32(i))
	}
	monitor, err := NewMonitorFromConfig(k.cfg, partitions)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create monitor instance")
	}
	k.monitor = monitor
	monitorCtx, monitorCancel := context.WithCancel(k.rootCtx)
	k.monitorCancelFunc = monitorCancel
	go k.monitor.Start(monitorCtx)
}
