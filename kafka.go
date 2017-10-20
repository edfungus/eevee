package eevee

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaConnectionConfig configures the connection to Kafka and defines what topics are accessed
type KafkaConnectionConfig struct {
	Server   string
	Topics   []string
	ClientID string
}

// KafkaConnection manages and abstracts the connection to Kafka
type KafkaConnection struct {
	consumer *kafka.Consumer
	producer *kafka.Producer
	config   KafkaConnectionConfig
	in       chan Payload
	out      chan Payload
}

// NewKafkaConnection returns a new object connected to Kafka with specific topics
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
		consumer: c,
		producer: p,
		config:   config,
		in:       make(chan Payload),
		out:      make(chan Payload),
	}, nil
}

// Start begins sending and receiving Kafka messages. The context is used to stop the Kafka clients
func (kc *KafkaConnection) Start(ctx context.Context) {
	err := kc.consumer.SubscribeTopics(kc.config.Topics, nil)
	if err != nil {
		log.Fatal("Kakfa could not subscribe to topics")
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
			switch e := event.(type) {
			case *kafka.Message:
				if e.TopicPartition.Topic == nil {
					log.Criticalf("Topic was nil?")
					continue
				}
				log.Debug("Kafka received message")
				kc.in <- NewPayload(e.Value, *e.TopicPartition.Topic)
			case kafka.Error:
				log.Errorf("Kafka error: %v", e)
			case kafka.AssignedPartitions:
				log.Infof("Kafka parition: %v", e)
				kc.consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				log.Infof("Kafka parition: %v", e)
				kc.consumer.Unassign()
			case kafka.PartitionEOF:
				// log.Infof("Kafka parition: %v", e)
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
			kc.producer.ProduceChannel() <- kc.createKafkaMessage(payload)
		}
	}
}

func (kc *KafkaConnection) createKafkaMessage(payload Payload) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &payload.Topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload.RawMessage,
	}
}
