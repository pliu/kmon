package config

import (
	"encoding/json"
)

type KMonConfig struct {
	ProducerKafkaConfig             *KafkaConfig `json:"producer_kafka_config" validate:"required"`
	ConsumerKafkaConfig             *KafkaConfig
	ProducerMonitoringTopic         string `json:"producer_monitoring_topic" validate:"required,min=1"`
	ConsumerMonitoringTopic         string
	SampleFrequencyMs               int
	StatsWindowSeconds              int
	TopicReconciliationFrequencyMin int
}

type KafkaConfig struct {
	SeedBrokers []string `json:"seed_brokers" validate:"required,min=1,dive,min=1"`
}

func (cfg *KMonConfig) GetSampleFrequencyMs() int {
	if cfg.SampleFrequencyMs != 0 {
		return cfg.SampleFrequencyMs
	}
	return 100
}

func (cfg *KMonConfig) GetStatsWindowSeconds() int {
	if cfg.StatsWindowSeconds != 0 {
		return cfg.StatsWindowSeconds
	}
	return 60
}

func (cfg *KMonConfig) GetTopicReconciliationFrequencyMin() int {
	if cfg.TopicReconciliationFrequencyMin != 0 {
		return cfg.TopicReconciliationFrequencyMin
	}
	return 60
}

func GetKMonConfigFromBytes(data *[]byte) (*KMonConfig, error) {
	var cfg KMonConfig
	err := json.Unmarshal(*data, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
