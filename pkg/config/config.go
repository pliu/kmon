package config

type KMonConfig struct {
	KafkaConfig       KafkaConfig
	SampleFrequencyMs int
}

type KafkaConfig struct {
	SeedBrokers []string
}
