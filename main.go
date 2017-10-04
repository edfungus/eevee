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
	mqtt "github.com/eclipse/paho.mqtt.golang"
	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("eevee")

var (
	topicKAFKA           = "kafka-in-out"
	topicMQTT            = "mqtt-in-out"
	mqttIncomingMessages = make(chan mqtt.Message)
)

type Payload struct {
	ID      int
	Message []byte
	Topic   string
}

type Connect interface {
	In() <-chan Payload
	Out() chan<- Payload
}

func main() {
	startLogger()
	kafkaConfig := KafkaConnectConfig{
		Server:   "localhost:9092",
		Topics:   []string{topicKAFKA},
		ClientID: "eevee",
	}
	kafkaConnect, err := NewKafkaConnect(kafkaConfig)
	if err != nil {
		log.Fatalf("Could not create Kafka Client with err: %s", err.Error())
	}
	mqttConnect := NewMqttConnect()
	bridge := NewBidiBridge(kafkaConnect, mqttConnect)
	bridge.Start()

	// MQTT Client
	mqttOptions := mqtt.NewClientOptions().SetClientID("eevee").AddBroker("tcp://localhost:1883")
	mClient := mqtt.NewClient(mqttOptions)
	if token := mClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to connect to MQTT broker: %s", token.Error())
	}
	mqttMessageHandler := func(client mqtt.Client, message mqtt.Message) {
		mqttIncomingMessages <- message
	}
	mClient.Subscribe(topicMQTT, byte(0), mqttMessageHandler)

	// Starting the listening process!
	log.Noticef("Eevee is starting up")

	ctx := context.Background()
	ctx, cancelCtx := context.WithCancel(ctx)
	ctxC, cancelCtxC := context.WithCancel(ctx)
	defer cancelCtxC()

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
			case message := <-mqttIncomingMessages:
				log.Critical("IN MQTT TOPIC")
				processed := checkKeyInHashsetInt(processedFromKafkaKeys, int(message.MessageID()))
				if !processed {
					key, number := generateKey()
					log.Info("NUMBER GEN", "number", number)
					log.Info("KEY OUT", "key", key)
					addKeyToHashset(processedFromMQTTKeys, number)
					log.Criticalf("CHECK HASHSET", "here?", processedFromMQTTKeys[number])
					forwardToKafkaInOut(p, topicKAFKA, message.Payload(), key)
				} else {
					log.Noticef("MQTT | value: %s | key: %d ", string(message.Payload()), message.MessageID())
					removeKeyFromHashsetInt(processedFromKafkaKeys, int(message.MessageID()))
				}
			case event := <-c.Events():
				switch e := event.(type) {
				case *kafka.Message:
					if e.TopicPartition.Topic == nil {
						log.Criticalf("Topic was nil?")
						continue
					}
					log.Critical("IN KAFKA TOPIC")
					processed := checkKeyInHashset(processedFromMQTTKeys, e.Key)
					if !processed {
						token := mClient.Publish(topicMQTT, byte(0), false, e.Value)
						key := int(token.(*mqtt.PublishToken).MessageID())
						log.Infof("MESSAGE KEY: %d", key)
						addKeyToHashset(processedFromKafkaKeys, key)
					} else {
						key := int(binary.LittleEndian.Uint16(e.Key))
						log.Noticef("Kafka | value: %s | key: %d ", string(e.Value), key)
						removeKeyFromHashset(processedFromMQTTKeys, e.Key)
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
	// should check against the map
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

// get rid of this copy pasta
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

func checkKeyInHashsetInt(hashset map[int]bool, number int) bool {
	log.Critical("NUMBER FIND", "number", number)
	log.Criticalf("HASHSET", "set", hashset)
	_, ok := hashset[number]
	return ok
}

func removeKeyFromHashset(hashset map[int]bool, key []byte) {
	number := int(binary.LittleEndian.Uint16(key))
	delete(hashset, number)
}

func removeKeyFromHashsetInt(hashset map[int]bool, number int) {
	delete(hashset, number)
}
