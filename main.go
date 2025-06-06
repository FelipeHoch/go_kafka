package main

import (
	"log"

	"github.com/felipehoch/go_kafka/internal/config"
	"github.com/felipehoch/go_kafka/internal/consumer"
	"github.com/felipehoch/go_kafka/internal/service"
)

func main() {
	cfg := config.NewConfig()

	kafkaConsumer, err := consumer.NewKafkaService(
		cfg.KafkaBootstrapServers, 
		cfg.KafkaGroupID,  
		cfg.KafkaTopicDLQ,
		cfg.KafkaUsername, 
		cfg.KafkaPassword, 
		cfg.KafkaSecurityProtocol, 
		cfg.KafkaSASLMechanism,
	)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %v", err)
	}
	
	orderService, err := service.NewOrderService(cfg.TargetServiceURL)
	if err != nil {
		log.Fatalf("Error creating order service: %v", err)
	}

	err = kafkaConsumer.Consume(cfg.KafkaTopic, orderService.ProcessMessage)
	if err != nil {
		log.Fatalf("Error consuming topic: %v", err)
	}

	log.Printf("Consuming topic: %s", cfg.KafkaTopic)
}