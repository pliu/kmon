package kmon

import (
	"context"
	"fmt"
	"sort"

	"github.com/pliu/kmon/pkg/config"
	"github.com/pliu/kmon/pkg/utils"
	"github.com/twmb/franz-go/pkg/kadm"
)

type TopicManager struct {
	config           config.KMonConfig
	kafkaAdminClient *kadm.Client
}

// PartitionLeaderMove describes a planned leader change for a single partition.
type PartitionLeaderMove struct {
	Partition     int32
	CurrentLeader int32
	TargetLeader  int32
}

// LeaderReassignmentPlan captures the set of leader moves required to satisfy leader coverage.
type LeaderReassignmentPlan struct {
	Topic string
	Moves []PartitionLeaderMove
}

func NewTopicManager(config config.KMonConfig, adminClient *kadm.Client) *TopicManager {
	return &TopicManager{
		config:           config,
		kafkaAdminClient: adminClient,
	}
}

// GetAllBrokers gets all unique broker IDs from both the admin client's list of brokers
// and from the replicas of all topic partitions.
func (tm *TopicManager) GetAllBrokers(ctx context.Context) ([]int32, error) {
	brokerIDs := utils.NewSet[int32]()

	brokerDetails, err := tm.kafkaAdminClient.ListBrokers(ctx)
	if err != nil {
		return nil, err
	}
	for id := range brokerDetails {
		brokerIDs.Add(int32(id))
	}

	topicDetails, err := tm.kafkaAdminClient.ListTopics(ctx)
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

	return brokerIDs.Values(), nil
}

// AddPartitions adds new partitions to an existing topic.
func (tm *TopicManager) AddPartitions(ctx context.Context, topicName string, partitionsToAdd int32) error {
	responses, err := tm.kafkaAdminClient.CreatePartitions(ctx, int(partitionsToAdd), topicName)
	if err != nil {
		return err
	}

	resp, err := responses.On(topicName, nil)
	if err != nil {
		return err
	}

	return resp.Err
}

// PlanLeaderCoverage ensures every broker has at least one leader for the topic by planning leader moves.

func (tm *TopicManager) PlanLeaderCoverage(ctx context.Context, topicName string, brokersSet *utils.Set[int32]) (LeaderReassignmentPlan, error) {
	plan := LeaderReassignmentPlan{Topic: topicName}

	brokers := brokersSet.Values()
	sort.Slice(brokers, func(i, j int) bool { return brokers[i] < brokers[j] })

	if len(brokers) == 0 {
		return plan, nil
	}

	topicDetails, err := tm.kafkaAdminClient.ListTopics(ctx, topicName)
	if err != nil {
		return plan, fmt.Errorf("list topics for %s: %w", topicName, err)
	}

	td, ok := topicDetails[topicName]
	if !ok {
		return plan, fmt.Errorf("topic %s not found", topicName)
	}
	if td.Err != nil {
		return plan, fmt.Errorf("load topic metadata for %s: %w", topicName, td.Err)
	}
	if len(td.Partitions) == 0 {
		return plan, fmt.Errorf("topic %s has no partitions", topicName)
	}
	if len(td.Partitions) < len(brokers) {
		return plan, fmt.Errorf("insufficient partitions to cover brokers for topic %s", topicName)
	}

	adjacency := make(map[int32][]int32, len(brokers))
	for _, id := range brokers {
		adjacency[id] = nil
	}

	for _, pd := range td.Partitions {
		if pd.Err != nil {
			return plan, fmt.Errorf("partition %d metadata error: %w", pd.Partition, pd.Err)
		}
		for _, replica := range pd.Replicas {
			if brokersSet.Contains(replica) {
				adjacency[replica] = append(adjacency[replica], pd.Partition)
			}
		}
	}

	for broker, partitions := range adjacency {
		if len(partitions) == 0 {
			return plan, fmt.Errorf("broker %d is not a replica for topic %s", broker, topicName)
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
		adjacency[broker] = partitions
	}

	matchPartition := make(map[int32]int32)
	var dfs func(int32, map[int32]bool) bool
	dfs = func(broker int32, visited map[int32]bool) bool {
		for _, partition := range adjacency[broker] {
			if visited[partition] {
				continue
			}
			visited[partition] = true
			if current, ok := matchPartition[partition]; !ok || dfs(current, visited) {
				matchPartition[partition] = broker
				return true
			}
		}
		return false
	}

	for _, broker := range brokers {
		if !dfs(broker, make(map[int32]bool)) {
			return plan, fmt.Errorf("unable to plan leader coverage for topic %s", topicName)
		}
	}

	brokerAssignment := make(map[int32]int32, len(brokers))
	for partition, broker := range matchPartition {
		brokerAssignment[broker] = partition
	}
	if len(brokerAssignment) != len(brokers) {
		return plan, fmt.Errorf("incomplete leader coverage plan for topic %s", topicName)
	}

	for _, broker := range brokers {
		partition, ok := brokerAssignment[broker]
		if !ok {
			return plan, fmt.Errorf("missing partition assignment for broker %d on topic %s", broker, topicName)
		}
		pd := td.Partitions[partition]
		if pd.Leader == broker {
			continue
		}
		plan.Moves = append(plan.Moves, PartitionLeaderMove{
			Partition:     partition,
			CurrentLeader: pd.Leader,
			TargetLeader:  broker,
		})
	}

	return plan, nil
}
