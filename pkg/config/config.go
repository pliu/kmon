package config

import (
	"encoding/json"
)

type KMonConfig struct {
	ProducerKafkaConfig             *KafkaConfig `json:"producerKafkaConfig" validate:"required"`
	ConsumerKafkaConfig             *KafkaConfig `json:"consumerKafkaConfig,omitempty"`
	ProducerMonitoringTopic         string       `json:"producerMonitoringTopic" validate:"required,min=1"`
	ConsumerMonitoringTopic         string       `json:"consumerMonitoringTopic,omitempty"`
	SampleFrequencyMs               int          `json:"sampleFrequencyMs,omitempty"`
	StatsWindowSeconds              int          `json:"statsWindowSeconds,omitempty"`
	TopicReconciliationFrequencyMin int          `json:"topicReconciliationFrequencyMin,omitempty"`
}

type KafkaConfig struct {
	SeedBrokers []string `json:"seedBrokers" validate:"required,min=1,dive,min=1"`
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

func (cfg *KMonConfig) String() string {
	data, _ := json.Marshal(cfg)
	return string(data)
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
