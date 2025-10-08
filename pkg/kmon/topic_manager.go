package kmon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pliu/kmon/pkg/clients"
	"github.com/pliu/kmon/pkg/config"
	"github.com/pliu/kmon/pkg/utils"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO: Figure out how topic manager manages topics across clusters for cross-cluster measurement
// TODO: Figure out how to get partition list out of this object back up to the kmon object
type TopicManager struct {
	client                  *kgo.Client
	admClient               *kadm.Client
	topicName               string
	reconciliationInterval  time.Duration
	previousBrokerSet       *utils.Set[int32]
	changeDetectedCallback  func()
	doneReconcilingCallback func()
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
	}, nil
}

func (tm *TopicManager) Start(ctx context.Context) {
	defer tm.admClient.Close()

	ticker := time.NewTicker(tm.reconciliationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = tm.maybeReconcileTopic(ctx)
		}
	}
}

func (tm *TopicManager) maybeReconcileTopic(ctx context.Context) error {
	partitions, err := tm.getTopicPartitions(ctx)
	if err != nil {
		return err
	}

	brokerIDs, err := tm.getAllBrokers(ctx)
	if err != nil {
		return err
	}

	if partitions == nil {
		tm.changeDetectedCallback()
		for err = tm.createTopic(ctx, brokerIDs); err != nil; {
			time.Sleep(1 * time.Second)
			err = tm.createTopic(ctx, brokerIDs)
		}
		tm.waitUntilTopicExists((ctx))
		tm.doneReconcilingCallback()
		return nil
	}

	if len(partitions) != brokerIDs.Len() || !brokerIDs.Equals(tm.previousBrokerSet) {
		tm.changeDetectedCallback()
		_ = tm.reconcileTopic(ctx, brokerIDs)
		tm.doneReconcilingCallback()
	}

	return nil
}

func (tm *TopicManager) getTopicPartitions(ctx context.Context) ([]int32, error) {
	topicDetails, err := tm.admClient.ListTopics(ctx, tm.topicName)
	if err != nil {
		return nil, err
	}

	if td, exists := topicDetails[tm.topicName]; exists {
		if td.Err == nil {
			return td.Partitions.Numbers(), nil
		}
		var kafkaErr *kerr.Error
		if errors.As(td.Err, &kafkaErr) {
			if kafkaErr == kerr.UnknownTopicOrPartition {
				return nil, nil
			}
		}
		return nil, td.Err
	}

	return nil, nil
}

func (tm *TopicManager) createTopic(ctx context.Context, brokerIDs *utils.Set[int32]) error {
	createTopicsRequest := kmsg.NewCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = tm.topicName
	topic.NumPartitions = -1
	topic.ReplicationFactor = -1
	topic.Configs = tm.generateTopicConfigs()
	topic.ReplicaAssignment = tm.generatePartitionAssignment(brokerIDs)
	createTopicsRequest.Topics = append(createTopicsRequest.Topics, topic)

	resp, err := createTopicsRequest.RequestWith(ctx, tm.client)
	if err != nil {
		return err
	}
	for _, tr := range resp.Topics {
		if tr.ErrorCode != 0 {
			return fmt.Errorf("%s", *tr.ErrorMessage)
		}
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
	for i, brokerIDs := range brokerIDs.Items() {
		replicaAssignment := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
		replicaAssignment.Partition = int32(i)
		replicaAssignment.Replicas = []int32{brokerIDs}
		replicaAssignments = append(replicaAssignments, replicaAssignment)
	}
	return replicaAssignments
}

// This waits for 5 successes as each ListTopics is a metadata call to a random broker.
// Writes (e.g., create, delete) go through the coordinator but take time to propogate to other brokers, resulting in eventual consistency.
func (tm *TopicManager) waitUntilTopicExists(ctx context.Context) {
	for i := 0; i < 5; {
		topics, err := tm.admClient.ListTopics(ctx, tm.topicName)
		if err == nil {
			td, exists := topics[tm.topicName]
			if exists && td.Err == nil {
				i += 1
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (tm *TopicManager) waitUntilTopicNoLongerExists(ctx context.Context) {
	for i := 0; i < 5; {
		topics, err := tm.admClient.ListTopics(ctx)
		if err == nil {
			td, exists := topics[tm.topicName]
			if !exists || errors.Is(td.Err, kerr.UnknownTopicOrPartition) {
				i += 1
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (tm *TopicManager) reconcileTopic(ctx context.Context, brokerIDs *utils.Set[int32]) error {
	_, err := tm.admClient.DeleteTopic(ctx, tm.topicName)
	if err != nil {
		return err
	}
	tm.waitUntilTopicNoLongerExists(ctx)
	for err = tm.createTopic(ctx, brokerIDs); err != nil; {
		time.Sleep(1 * time.Second)
		err = tm.createTopic(ctx, brokerIDs)
	}
	tm.waitUntilTopicExists(ctx)
	return nil
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
