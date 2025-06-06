package consumer

import (
	"strings"
	"time"
	"log"
	"errors"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/avast/retry-go/v4"
)

type KafkaConsumer struct {
	consumer         *kafka.Consumer
	brokers          []string
	groupID          string
	topic            string
	dlqTopic         string
	username         string
	password         string
	securityProtocol string
	saslMechanism    string
}

func NewKafkaConsumer(brokers []string, groupID string, dlqTopic string, username string, password string, securityProtocol string, saslMechanism string) (*KafkaConsumer, error) {
	kc := &KafkaConsumer{
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

	return kc, nil
}

func (c *KafkaConsumer) Consume(topic string, callback func(message *kafka.Message) error) error {
	c.topic = topic

	c.consumer.Subscribe(topic, nil)
	
	log.Printf("Consuming topic: %s", topic)
	
	for {
		if c.consumer == nil {
			if err := c.reconnect(); err != nil {
				time.Sleep(10 * time.Second)

				continue
			}
			c.consumer.Subscribe(topic, nil)
		}
		
		msg, err := c.consumer.ReadMessage(time.Second)
		
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.IsTimeout() {
				continue
			}
			
			if err := c.reconnect(); err != nil {
				c.consumer = nil

				log.Printf("Connection failed: %v", err)
				
				time.Sleep(10 * time.Second)
			}

			continue
		}
		
		if err := callback(msg); err != nil {
			log.Printf("Error processing: %v", err)
		}
	}
}

func (c *KafkaConsumer) Close() error {
	if c.consumer != nil {
		return c.consumer.Close()
	}
	return nil
}

func (c *KafkaConsumer) connect() error {
	log.Printf("Connecting to Kafka: %s", strings.Join(c.brokers, ","))
	
	config := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(c.brokers, ","),
		"group.id":          c.groupID,
		"auto.offset.reset": "earliest",
		
	}
	
	if c.securityProtocol != "" {
		config.SetKey("security.protocol", c.securityProtocol)
	}
	
	if c.saslMechanism != "" {
		config.SetKey("sasl.mechanisms", c.saslMechanism)
	}
	
	if c.username != "" {
		config.SetKey("sasl.username", c.username)
	}
	
	if c.password != "" {
		config.SetKey("sasl.password", c.password)
	}
	
	consumer, err := kafka.NewConsumer(config)
	
	if err != nil {
		return err
	}

	c.consumer = consumer

	metadata, err := consumer.GetMetadata(nil, false, 5000)
	if err != nil {
		return err
	}

	if len(metadata.Brokers) == 0 {
		return errors.New("no broker available")
	}

	log.Printf("Connected to Kafka successfully")

	return nil
}

func (c *KafkaConsumer) reconnect() error {
	if c.consumer != nil {
		c.consumer.Close()
	}
	
	return retry.Do(
		func() error {
			return c.connect()
		},
		retry.Attempts(3),
		retry.Delay(2*time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.OnRetry(func(n uint, err error) {
			log.Printf("Reconnecting... attempt %d", n+1)
		}),
	)
}