package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("eevee")

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

	topic := "test-topic"
	err = c.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("Could not subsribe to topics: %s", topic)
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

	runP:
		for {
			select {
			case <-ticker.C:
				log.Infof("Sending a message...")
				p.ProduceChannel() <- &kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &topic,
						Partition: kafka.PartitionAny,
					},
					Value: []byte("hello!"),
				}
			case <-ctxP.Done():
				log.Noticef("Producer is shutting down")
				break runP
			}
		}
	}()

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
					op, ok := e.Opaque.([]byte)
					if !ok {
						op = []byte("")
					}
					log.Noticef("Got a message: %s | key: %s | opaque: %s", string(e.Value), string(e.Key), string(op))
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
	for {
		<-sigchan
		cancelCtx()
		time.Sleep(time.Second * 1)
		break
	}

	log.Noticef("Eevee says goodbye")

}

func startLogger() {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	format := logging.MustStringFormatter(`%{color}%{shortfunc} ▶ %{level:.4s} %{color:reset} %{message}`)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)
}
