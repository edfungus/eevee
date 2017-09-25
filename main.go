package main

import (
	"context"
	"encoding/binary"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("eevee")

type opaque struct {
	Value string
}

func main() {
	startLogger()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               "localhost:9092",
		"group.id":                        "test",
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})
	// config link: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer c.Close()

	topicKAFKA := "kafka-in-out"
	topicMQTT := "mqtt-in-out"
	err = c.SubscribeTopics([]string{topicKAFKA, topicMQTT}, nil)
	if err != nil {
		log.Fatal("Could not subsribe to topics:")
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	log.Noticef("Eevee is starting up")

	ctx := context.Background()
	ctx, cancelCtx := context.WithCancel(ctx)
	ctxC, cancelCtxC := context.WithCancel(ctx)
	defer cancelCtxC()
	ctxP, cancelCtxP := context.WithCancel(ctx)
	defer cancelCtxP()

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		log.Noticef("Producer is starting up")
		i := 0
	runP:
		for {
			select {
			case <-ticker.C:
				// log.Infof("Sending a message...")
				// p.ProduceChannel() <- &kafka.Message{
				// 	TopicPartition: kafka.TopicPartition{
				// 		Topic:     &topic,
				// 		Partition: kafka.PartitionAny,
				// 	},
				// 	Value: []byte("hello!"),
				// 	Key:   []byte(fmt.Sprintf("Op%d", i)),
				// }
			case <-ctxP.Done():
				log.Noticef("Producer is shutting down")
				break runP
			}
			i++
		}
	}()

	processedFromMQTTKeys := make(map[int]bool)
	processedFromKafkaKeys := make(map[int]bool)

	go func() {
		log.Noticef("Consumer is starting up")

	runC:
		for {
			select {
			case <-ctxC.Done():
				log.Noticef("Consumer is shutting down")
				break runC
			case event := <-c.Events():
				switch e := event.(type) {
				case *kafka.Message:
					if e.TopicPartition.Topic == nil {
						log.Criticalf("Topic was nil?")
						continue
					}
					// continue
					topic := *e.TopicPartition.Topic
					switch topic {
					case topicMQTT:
						log.Critical("IN MQTT TOPIC")
						processed := checkKeyInHashset(processedFromKafkaKeys, e.Key)
						if !processed {
							key, number := generateKey()
							log.Info("NUMBER GEN", "number", number)
							log.Info("KEY OUT", "key", key)
							addKeyToHashset(processedFromMQTTKeys, number)
							log.Criticalf("CHECK HASHSET", "here?", processedFromMQTTKeys[number])
							forwardToKafkaInOut(p, topicKAFKA, e.Value, key)
						} else {
							log.Noticef("MQTT | value: %s | key: %s ", string(e.Value), string(e.Key))
							removeKeyFromHashset(processedFromKafkaKeys, e.Key)
						}
					case topic:
						log.Critical("IN KAFKA TOPIC")
						processed := checkKeyInHashset(processedFromMQTTKeys, e.Key)
						if !processed {
							number := rand.Int()
							key, number := generateKey()
							log.Info("NUMBER GEN", "number", number)
							log.Info("KEY OUT", "key", key)
							addKeyToHashset(processedFromKafkaKeys, number)
							forwardToKafkaInOut(p, topicMQTT, e.Value, key)
						} else {
							log.Noticef("Kafka | value: %s | key: %s ", string(e.Value), string(e.Key))
							removeKeyFromHashset(processedFromMQTTKeys, e.Key)
						}
					}
				case kafka.AssignedPartitions:
					log.Infof("%% %v\n", e)
					c.Assign(e.Partitions)
				case kafka.RevokedPartitions:
					log.Infof("%% %v\n", e)
					c.Unassign()
				case kafka.PartitionEOF:
					log.Infof("%% Reached %v\n", e)
				case kafka.Error:
					log.Errorf("%% Error: %v\n", e)
				}
			}
		}
	}()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	<-sigchan
	cancelCtx()
	time.Sleep(time.Second * 1)
	log.Noticef("Eevee says goodbye")

}

func startLogger() {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	format := logging.MustStringFormatter(`%{color}%{shortfunc} ▶ %{level:.4s} %{color:reset} %{message}`)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)
}

func generateKey() (token []byte, number int) {
	token = make([]byte, 4)
	rand.Read(token)
	number = int(binary.LittleEndian.Uint16(token))
	return token, number
}

func addKeyToHashset(hashset map[int]bool, key int) {
	hashset[key] = true
}

func forwardToKafkaInOut(p *kafka.Producer, topic string, value []byte, key []byte) {
	log.Critical("SENDING MESSAGE TO", "topic", topic)
	p.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
		Key:   key,
	}
}

func checkKeyInHashset(hashset map[int]bool, key []byte) bool {
	log.Info("KEY IN", "key", key)
	log.Info("KEY LENGTH", "key", len(key))
	if len(key) == 0 {
		return false
	}
	number := int(binary.LittleEndian.Uint16(key))
	log.Critical("NUMBER FIND", "number", number)
	log.Criticalf("HASHSET", "set", hashset)
	_, ok := hashset[number]
	return ok
}

func removeKeyFromHashset(hashset map[int]bool, key []byte) {
	number := int(binary.LittleEndian.Uint16(key))
	delete(hashset, number)
}
