package main

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaConnectConfig configures to connect to Kafka and defines what topics are accessed
type KafkaConnectConfig struct {
	Server   string
	Topics   []string
	ClientID string
}

// KafkaConnect manages and abstracts the connection to Kafka
type KafkaConnect struct {
	consumer *kafka.Consumer
	producer *kafka.Producer
	config   KafkaConnectConfig
	in       chan Payload
	out      chan Payload
}

// NewKafkaConnect returns a new object connected to Kafka with specific topics
func NewKafkaConnect(config KafkaConnectConfig) (*KafkaConnect, error) {
	// config link: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               config.Server,
		"group.id":                        config.ClientID,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
		return nil, err
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": config.Server})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
		return nil, err
	}

	return &KafkaConnect{
		consumer: c,
		producer: p,
		config:   config,
		in:       make(chan Payload),
		out:      make(chan Payload),
	}, nil
}

// Start begins send and receiving Kafka messages. The context is used to stop the Kafka client
func (kc *KafkaConnect) Start(ctx context.Context) error {
	err := kc.consumer.SubscribeTopics(kc.config.Topics, nil)
	if err != nil {
		log.Fatal("Could not subsribe to topics:")
		return err
	}

	go kc.receiveMessages(ctx)
	go kc.sendMessages(ctx)
	return nil
}

// In recieves the message subscribe from Kafka
func (kc *KafkaConnect) In() <-chan Payload {
	return kc.in
}

// Out sends messages out to Kafka
func (kc *KafkaConnect) Out() chan<- Payload {
	return kc.out
}

func (kc *KafkaConnect) receiveMessages(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			log.Debug("Stopping Kafka subscriber")
			kc.consumer.Close()
			break loop
		case event := <-kc.consumer.Events():
			switch e := event.(type) {
			case *kafka.Message:
				if e.TopicPartition.Topic == nil {
					log.Criticalf("Topic was nil?")
					continue
				}
				payload := Payload{
					ID:      bytes2int(e.Key),
					Message: e.Value,
					Topic:   *e.TopicPartition.Topic,
				}
				kc.in <- payload
			}
		}
	}
}

func (kc *KafkaConnect) sendMessages(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			log.Debug("Stopping Kafka publisher")
			kc.producer.Close()
			break loop
		case payload := <-kc.out:
			kc.producer.ProduceChannel() <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &payload.Topic,
					Partition: kafka.PartitionAny,
				},
				Value: payload.Message,
				Key:   int2bytes(payload.ID),
			}
		}
	}
}
