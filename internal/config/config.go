package config

import (
	"log"

	"github.com/spf13/viper"
)

type Config struct {
	KafkaBootstrapServers []string `mapstructure:"KAFKA_BOOTSTRAP_SERVERS"`
	KafkaUsername         string   `mapstructure:"KAFKA_USERNAME"`
	KafkaPassword         string   `mapstructure:"KAFKA_PASSWORD"`
	KafkaSASLMechanism    string   `mapstructure:"KAFKA_SASL_MECHANISM"`
	KafkaSecurityProtocol string   `mapstructure:"KAFKA_SECURITY_PROTOCOL"`
	KafkaGroupID          string   `mapstructure:"KAFKA_GROUP_ID"`
	KafkaTopic            string   `mapstructure:"KAFKA_TOPIC"`
	KafkaTopicDLQ         string   `mapstructure:"KAFKA_TOPIC_DLQ"`
	TargetServiceURL      string   `mapstructure:"TARGET_SERVICE_URL"`
}

func NewConfig() *Config {
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./")
	
	viper.AutomaticEnv()
	
	viper.SetDefault("KAFKA_BOOTSTRAP_SERVERS", []string{"localhost:9092"})
	viper.SetDefault("KAFKA_SASL_MECHANISM", "PLAIN")
	viper.SetDefault("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
	viper.SetDefault("KAFKA_GROUP_ID", "seu-grupo")
	viper.SetDefault("KAFKA_TOPIC", "seu-topico")
	viper.SetDefault("TARGET_SERVICE_URL", "http://localhost:8080/api/v1")
	viper.SetDefault("KAFKA_TOPIC_DLQ", "seu-topico-dlq")
	viper.SetDefault("KAFKA_USERNAME", "exemplo")
	viper.SetDefault("KAFKA_PASSWORD", "exemplo")	
	
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("Error reading configuration file: %v", err)

		log.Println("Using environment variables or default values")
	}
	
	var config Config
	
	if err := viper.Unmarshal(&config); err != nil {
		log.Fatalf("Error unmarshalling configurations: %v", err)
	}
	
	return &config
}