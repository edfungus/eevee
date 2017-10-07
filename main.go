package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("eevee")

var (
	topics = []string{"topic1", "topic2"}
)

type Connection interface {
	Start(ctx context.Context)
	In() <-chan Payload
	Out() chan<- Payload
	IDStore() IDStore
}

func main() {
	startLogger()
	log.Noticef("Eevee is starting up")

	kafkaConfig := KafkaConnectionConfig{
		Server:   "localhost:9092",
		Topics:   topics,
		ClientID: "eevee",
	}
	kafkaConn, err := NewKafkaConnection(kafkaConfig)
	if err != nil {
		log.Fatalf("Could not create Kafka Client with err: %s", err.Error())
	}

	mqttConfig := MqttConnectionConfig{
		Server:   "tcp://localhost:1883",
		Topics:   topics,
		ClientID: "eevee",
		Qos:      byte(1),
	}
	mqttConn, err := NewMqttConnection(mqttConfig)
	if err != nil {
		log.Fatalf("Could not create MQTT Client with err: %s", err.Error())
	}

	bridge := NewBidiBridge(kafkaConn, mqttConn)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	bridge.Start(ctx)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	<-sigchan
	cancel()
	time.Sleep(time.Second * 1)
	log.Noticef("Eevee says goodbye")

}

func startLogger() {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	format := logging.MustStringFormatter(`%{color}%{shortfunc} ▶ %{level:.4s} %{color:reset} %{message}`)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)
}
