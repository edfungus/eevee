package main

import (
	"context"
	"math/rand"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaConnectConfig configures the connection to Kafka and defines what topics are accessed
type KafkaConnectionConfig struct {
	Server   string
	Topics   []string
	ClientID string
}

// KafkaConnect manages and abstracts the connection to Kafka
type KafkaConnection struct {
	consumer       *kafka.Consumer
	producer       *kafka.Producer
	config         KafkaConnectionConfig
	in             chan Payload
	out            chan Payload
	markedPayloads map[int]bool
}

// NewKafkaConnect returns a new object connected to Kafka with specific topics
func NewKafkaConnection(config KafkaConnectionConfig) (*KafkaConnection, error) {
	// config link: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               config.Server,
		"group.id":                        config.ClientID,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})
	if err != nil {
		log.Fatalf("Kafka failed to create consumer: %s", err)
		return nil, err
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": config.Server})
	if err != nil {
		log.Fatalf("Kafka failed to create producer: %s", err)
		return nil, err
	}

	return &KafkaConnection{
		consumer:       c,
		producer:       p,
		config:         config,
		in:             make(chan Payload),
		out:            make(chan Payload),
		markedPayloads: make(map[int]bool),
	}, nil
}

// Start begins sending and receiving Kafka messages. The context is used to stop the Kafka clients
func (kc *KafkaConnection) Start(ctx context.Context) {
	err := kc.consumer.SubscribeTopics(kc.config.Topics, nil)
	if err != nil {
		log.Fatal("Kakfa could not subsribe to topics")
	}

	go kc.receiveMessages(ctx)
	go kc.sendMessages(ctx)
	log.Info("Kafka client has started")
}

// In recieves the message subscribe from Kafka
func (kc *KafkaConnection) In() <-chan Payload {
	return kc.in
}

// Out sends messages out to Kafka
func (kc *KafkaConnection) Out() chan<- Payload {
	return kc.out
}

// In Kafka, we will get messages we send on to a topic we are also subscribing on, so we need to de-duplicate the messages
func (kc *KafkaConnection) GenerateID(payload Payload) Payload {
	if payload.ID == 0 {
		payload.ID = rand.Int()
	}
	return payload
}

func (kc *KafkaConnection) MarkPayload(payload Payload) {
	kc.markedPayloads[payload.ID] = true
}

func (kc *KafkaConnection) UnmarkPayload(payload Payload) {
	delete(kc.markedPayloads, payload.ID)
}

func (kc *KafkaConnection) IsDuplicate(payload Payload) bool {
	if kc.markedPayloads[payload.ID] {
		log.Debug("Kafka got duplicate message")
	}
	return kc.markedPayloads[payload.ID]
}

func (kc *KafkaConnection) receiveMessages(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			log.Info("Stopping Kafka consumer")
			kc.consumer.Close()
			close(kc.in)
			break loop
		case event := <-kc.consumer.Events():
			log.Debug("Something from Kafka....")
			switch e := event.(type) {
			case *kafka.Message:
				if e.TopicPartition.Topic == nil {
					log.Criticalf("Topic was nil?")
					continue
				}
				log.Debug("Kafka received message")
				payload := Payload{
					ID:      bytes2int(e.Key),
					Message: e.Value,
					Topic:   *e.TopicPartition.Topic,
				}
				kc.in <- payload
			case kafka.Error:
				log.Errorf("Kafka error: %v", e)
			}
		}
	}
}

func (kc *KafkaConnection) sendMessages(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			log.Info("Stopping Kafka publisher")
			kc.producer.Close()
			close(kc.out)
			break loop
		case payload := <-kc.out:
			log.Debug("Kafka sending message")
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
