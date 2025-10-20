package kmon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/phuslu/log"
	"github.com/pliu/kmon/pkg/clients"
	"github.com/pliu/kmon/pkg/config"
	"github.com/pliu/kmon/pkg/utils"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO: Figure out how topic manager manages topics across clusters for cross-cluster measurement
type TopicManager struct {
	client                  *kgo.Client
	admClient               *kadm.Client
	topicName               string
	reconciliationInterval  time.Duration
	previousBrokerSet       *utils.Set[int32]
	changeDetectedCallback  func()
	doneReconcilingCallback func(int)
	reconciling             bool
}

func NewTopicManagerFromConfig(cfg *config.KMonConfig) (*TopicManager, error) {
	client, err := clients.GetFranzGoClient(cfg.ProducerKafkaConfig)
	if err != nil {
		return nil, err
	}

	return &TopicManager{
		client:                 client,
		admClient:              kadm.NewClient(client),
		topicName:              cfg.ProducerMonitoringTopic,
		reconciliationInterval: time.Duration(cfg.GetTopicReconciliationFrequencyMin()) * time.Minute,
		reconciling:            false,
	}, nil
}

func (tm *TopicManager) Start(ctx context.Context) {
	defer tm.admClient.Close()

	ticker := time.NewTicker(tm.reconciliationInterval)
	defer ticker.Stop()

	for {
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 1*time.Minute)
		defer timeoutCancel()
		if err := tm.maybeReconcileTopic(timeoutCtx); err != nil {
			log.Error().Err(err).Msg("failed to reconcile topic - retrying in 5s")
			time.Sleep(5 * time.Second)
			continue
		}
		select {
		case <-ctx.Done():
			log.Info().Msg("Stopping TopicManager instance")
			return
		case <-ticker.C:
		}
	}
}

func (tm *TopicManager) maybeReconcileTopic(ctx context.Context) error {
	log.Info().Msg("Checking whether to reconcile topic")

	numPartitions, err := tm.getTopicNumPartitions(ctx)
	if err != nil {
		return err
	}

	brokerIDs, err := tm.getAllBrokers(ctx)
	if err != nil {
		return err
	}

	if tm.reconciling || numPartitions != brokerIDs.Len() || !brokerIDs.Equals(tm.previousBrokerSet) {
		tm.reconciling = true
		tm.changeDetectedCallback()
		if err = tm.reconcileTopic(ctx, brokerIDs); err != nil {
			return err
		}
		tm.doneReconcilingCallback(brokerIDs.Len())
		tm.reconciling = false
	}

	return nil
}

func (tm *TopicManager) getTopicNumPartitions(ctx context.Context) (int, error) {
	topicDetails, err := tm.admClient.ListTopics(ctx, tm.topicName)
	if err != nil {
		return 0, err
	}

	if td, exists := topicDetails[tm.topicName]; exists {
		if td.Err == nil {
			return len(td.Partitions.Numbers()), nil
		}
		if errors.Is(td.Err, kerr.UnknownTopicOrPartition) {
			return 0, nil
		}
		return 0, td.Err
	}

	return 0, nil
}

func (tm *TopicManager) createTopic(ctx context.Context, brokerIDs *utils.Set[int32]) error {
	log.Info().Msg("Creating topic")

	createTopicsRequest := kmsg.NewCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = tm.topicName
	topic.NumPartitions = -1
	topic.ReplicationFactor = -1
	topic.Configs = tm.generateTopicConfigs()
	topic.ReplicaAssignment = tm.generatePartitionAssignment(brokerIDs)
	createTopicsRequest.Topics = append(createTopicsRequest.Topics, topic)

	resp, err := createTopicsRequest.RequestWith(ctx, tm.client)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}
	if len(resp.Topics) != 1 {
		return fmt.Errorf("unexpected number of topics in response: %d", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != 0 && resp.Topics[0].ErrorCode != kerr.TopicAlreadyExists.Code {
		return fmt.Errorf("failed to create topic: %s", *resp.Topics[0].ErrorMessage)
	}

	tm.previousBrokerSet = brokerIDs

	return nil
}

func (tm *TopicManager) generateTopicConfigs() []kmsg.CreateTopicsRequestTopicConfig {
	topicConfigs := []kmsg.CreateTopicsRequestTopicConfig{}
	configs := map[string]string{
		"message.timestamp.type": "LogAppendTime",
		"min.insync.replicas":    "1",
		"retention.ms":           "1800000",
	}
	for k, v := range configs {
		topicConfig := kmsg.NewCreateTopicsRequestTopicConfig()
		topicConfig.Name = k
		topicConfig.Value = &v
		topicConfigs = append(topicConfigs, topicConfig)
	}
	return topicConfigs
}

// Partitions are given only 1 replica to avoid the leader automatically moving if the desired primary broker is down
// (this allows us to test individual brokers)
// TODO: Use more replicas for cross-cluster testing?
func (tm *TopicManager) generatePartitionAssignment(brokerIDs *utils.Set[int32]) []kmsg.CreateTopicsRequestTopicReplicaAssignment {
	replicaAssignments := []kmsg.CreateTopicsRequestTopicReplicaAssignment{}
	for i, brokerID := range brokerIDs.Items() {
		replicaAssignment := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
		replicaAssignment.Partition = int32(i)
		replicaAssignment.Replicas = []int32{brokerID}
		replicaAssignments = append(replicaAssignments, replicaAssignment)
	}
	return replicaAssignments
}

// This waits for 5 successes as each ListTopics is a metadata call to a random broker.
// Writes (e.g., create, delete) go through the coordinator but take time to propogate to other brokers, resulting in eventual consistency.
func (tm *TopicManager) waitUntilTopicExists(ctx context.Context) error {
	for i := 0; i < 5; {
		topics, err := tm.admClient.ListTopics(ctx, tm.topicName)
		if err == nil {
			td, exists := topics[tm.topicName]
			if exists && td.Err == nil {
				i += 1
			}
		} else if errors.Is(err, context.Canceled) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

func (tm *TopicManager) waitUntilTopicNoLongerExists(ctx context.Context) error {
	for i := 0; i < 5; {
		topics, err := tm.admClient.ListTopics(ctx)
		if err == nil {
			td, exists := topics[tm.topicName]
			if !exists || errors.Is(td.Err, kerr.UnknownTopicOrPartition) {
				i += 1
			}
		} else if errors.Is(err, context.Canceled) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

func (tm *TopicManager) reconcileTopic(ctx context.Context, brokerIDs *utils.Set[int32]) error {
	log.Info().Msg("Reconciling topic")

	if _, err := tm.admClient.DeleteTopic(ctx, tm.topicName); err != nil {
		if !errors.Is(err, kerr.UnknownTopicOrPartition) {
			return err
		}
	}
	if err := tm.waitUntilTopicNoLongerExists(ctx); err != nil {
		return err
	}
	if err := tm.createTopic(ctx, brokerIDs); err != nil {
		return err
	}
	return tm.waitUntilTopicExists(ctx)
}

// GetAllBrokers gets all unique broker IDs from both the admin client's list of brokers
// and from the replicas of all topic partitions to get a stable list as the client's list
// of brokers is only currently healthy brokers
func (tm *TopicManager) getAllBrokers(ctx context.Context) (*utils.Set[int32], error) {
	brokerIDs := utils.NewSet[int32]()

	brokerDetails, err := tm.admClient.ListBrokers(ctx)
	if err != nil {
		return nil, err
	}
	for _, bd := range brokerDetails {
		brokerIDs.Add(int32(bd.NodeID))
	}

	topicDetails, err := tm.admClient.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	for _, td := range topicDetails {
		for _, p := range td.Partitions {
			for _, r := range p.Replicas {
				brokerIDs.Add(r)
			}
		}
	}

	return brokerIDs, nil
}
