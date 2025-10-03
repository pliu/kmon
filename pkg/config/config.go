package config

type KMonConfig struct {
	ProducerKafkaConfig     *KafkaConfig
	ConsumerKafkaConfig     *KafkaConfig
	ProducerMonitoringTopic string
	ConsumerMonitoringTopic string
	SampleFrequencyMs       int
}

type KafkaConfig struct {
	SeedBrokers []string
}
