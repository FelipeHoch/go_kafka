package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaService struct {
	consumer         *kafka.Consumer
	producer         *kafka.Producer
	brokers          []string
	groupID          string
	topic            string
	dlqTopic         string
	username         string
	password         string
	securityProtocol string
	saslMechanism    string
}

func NewKafkaService(
	brokers []string,
	groupID string,
	dlqTopic string,
	username string,
	password string,
	securityProtocol string,
	saslMechanism string,
) (*KafkaService, error) {
	kc := &KafkaService{
		brokers:          brokers,
		groupID:          groupID,
		dlqTopic:         dlqTopic,
		username:         username,
		password:         password,
		securityProtocol: securityProtocol,
		saslMechanism:    saslMechanism,
	}

	err := kc.connect()
	if err != nil {
		return nil, err
	}

	producer, err := kc.createProducer()

	if err != nil {
		return nil, err
	}

	kc.producer = producer

	return kc, nil
}

func (c *KafkaService) Consume(topic string, callback func(message *kafka.Message) (string, error)) error {
	c.topic = topic

	if err := c.ensureTopicExists(topic); err != nil {
		return err
	}

	if err := c.consumer.Subscribe(topic, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %v", topic, err)
	}

	log.Printf("Successfully subscribed to topic: %s", topic)

	for {
		if c.consumer == nil {
			log.Printf("Consumer is nil, attempting reconnection...")

			if err := c.reconnectAndResubscribe(topic); err != nil {
				log.Printf("Reconnection failed: %v", err)

				time.Sleep(10 * time.Second)

				continue
			}
		}

		msg, err := c.consumer.ReadMessage(5 * time.Second)

		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok {
				if kafkaErr.IsTimeout() {
					continue
				}

				log.Printf("Kafka error: %v (code: %v)", kafkaErr, kafkaErr.Code())

				if c.requiresReconnection(kafkaErr) {
					if err := c.reconnectAndResubscribe(topic); err != nil {
						log.Printf("Reconnection failed, waiting 10 seconds: %v", err)

						time.Sleep(10 * time.Second)
					}
					continue
				}
			} else {
				log.Printf("Non-Kafka error: %v", err)

				time.Sleep(10 * time.Second)
			}
			continue
		}

		log.Printf("Message received: %v", string(msg.Value))

		traceID, processingErr := callback(msg)

		if processingErr != nil {
			log.Printf("Error processing: %v", processingErr)

			if dlqErr := c.PublishToDLQ(msg, traceID, processingErr); dlqErr != nil {
				log.Printf("Error publishing to DLQ: %v", dlqErr)
			} else {
				log.Printf("Published to DLQ")
			}
		} else {
			log.Printf("Message processed successfully")
		}

		if _, commitErr := c.consumer.CommitMessage(msg); commitErr != nil {
			log.Printf("Error committing message: %v", commitErr)
		}
	}
}

func (c *KafkaService) PublishToDLQ(message *kafka.Message, traceID string, err error) error {
	headers := make([]kafka.Header, 0)

	headers = append(headers, kafka.Header{
		Key:   "error",
		Value: []byte(err.Error()),
	})

	headers = append(headers, kafka.Header{
		Key:   "original-topic",
		Value: []byte(*message.TopicPartition.Topic),
	})

	headers = append(headers, kafka.Header{
		Key:   "failed-at",
		Value: []byte(time.Now().Format(time.RFC3339)),
	})

	headers = append(headers, kafka.Header{
		Key:   "trace-id",
		Value: []byte(traceID),
	})

	newMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &c.dlqTopic,
			Partition: kafka.PartitionAny,
		},
		Headers: headers,
		Value:   message.Value,
	}

	return c.producer.Produce(newMessage, nil)
}

func (c *KafkaService) Close() error {
	if c.consumer != nil {
		if err := c.consumer.Close(); err != nil {
			return err
		}

		c.consumer = nil
	}

	if c.producer != nil {
		c.producer.Close()

		c.producer = nil
	}

	return nil
}

func (c *KafkaService) connect() error {
	log.Printf("Connecting to Kafka: %s", strings.Join(c.brokers, ","))

	config := &kafka.ConfigMap{
		"bootstrap.servers":     strings.Join(c.brokers, ","),
		"group.id":              c.groupID,
		"auto.offset.reset":     "earliest",
		"enable.auto.commit":    "false",
		"session.timeout.ms":    10000,
		"heartbeat.interval.ms": 3000,
		"max.poll.interval.ms":  300000,
		"fetch.wait.max.ms":     500,
		"security.protocol":     c.securityProtocol,
		"sasl.mechanism":        c.saslMechanism,
		"sasl.username":         c.username,
		"sasl.password":         c.password,
	}

	consumer, err := kafka.NewConsumer(config)

	if err != nil {
		return err
	}

	c.consumer = consumer

	metadata, err := consumer.GetMetadata(nil, false, 10000)
	if err != nil {
		consumer.Close()
		return err
	}

	if len(metadata.Brokers) == 0 {
		consumer.Close()
		return errors.New("no broker available")
	}

	log.Printf("Connected to Kafka successfully with %d brokers", len(metadata.Brokers))

	return nil
}

func (c *KafkaService) reconnectAndResubscribe(topic string) error {
	log.Printf("Starting reconnection process...")

	if c.consumer != nil {
		c.consumer.Close()
		c.consumer = nil
	}

	err := retry.Do(
		func() error {
			return c.connect()
		},
		retry.Attempts(5),
		retry.Delay(2*time.Second),
		retry.MaxDelay(30*time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.OnRetry(func(n uint, err error) {
			log.Printf("Reconnecting... attempt %d: %v", n+1, err)
		}),
	)

	if err != nil {
		return fmt.Errorf("failed to reconnect after retries: %v", err)
	}

	if err := c.consumer.Subscribe(topic, nil); err != nil {
		return fmt.Errorf("failed to resubscribe to topic %s: %v", topic, err)
	}

	log.Printf("Successfully reconnected and resubscribed to topic: %s", topic)

	time.Sleep(2 * time.Second)

	return nil
}

func (c *KafkaService) createProducer() (*kafka.Producer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(c.brokers, ","),
		"security.protocol": c.securityProtocol,
		"sasl.mechanism":    c.saslMechanism,
		"sasl.username":     c.username,
		"sasl.password":     c.password,
	}

	producer, err := kafka.NewProducer(config)

	if err != nil {
		return nil, err
	}

	return producer, nil
}

func (c *KafkaService) ensureTopicExists(topic string) error {
	metadata, err := c.consumer.GetMetadata(nil, false, 5000)
	if err != nil {
		return err
	}

	log.Printf("Ensuring topic exists: %s", topic)

	for _, topicMetadata := range metadata.Topics {
		if topicMetadata.Topic == topic {
			log.Printf("Topic already exists: %s - %v", topic, topicMetadata)
			return nil
		}
	}

	adminClient, err := c.createAdminClient()
	if err != nil {
		return err
	}

	log.Printf("Creating topic: %s", topic)

	results, err := adminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}})
	if err != nil {
		return err
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return errors.New(result.Error.Error())
		}
	}

	log.Printf("Topic created: %s", topic)

	return nil
}

func (c *KafkaService) createAdminClient() (*kafka.AdminClient, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(c.brokers, ","),
		"security.protocol": c.securityProtocol,
		"sasl.mechanism":    c.saslMechanism,
		"sasl.username":     c.username,
		"sasl.password":     c.password,
	}

	return kafka.NewAdminClient(config)
}

func (c *KafkaService) requiresReconnection(kafkaErr kafka.Error) bool {
	switch kafkaErr.Code() {
	case kafka.ErrState,
		kafka.ErrTransport,
		kafka.ErrAllBrokersDown,
		kafka.ErrNetworkException,
		kafka.ErrBrokerNotAvailable:
		return true
	default:
		return kafkaErr.IsFatal()
	}
}
