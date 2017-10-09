package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/edfungus/eevee"
	logging "github.com/op/go-logging"
)

/*
A naive example with the basic parts showing how eevee works by routing messages between Kafka and MQTT for the topics listed.

Notes:
The id storage does not check for id reuse and is not a distributed nor a thread safe solution.
The json translator expects the same json structure from both Kafka and MQTT. Nothing fancy.
*/

var log = logging.MustGetLogger("eevee")

var (
	topics = []string{"topic1", "topic2"}
)

func main() {
	eevee.StartLogger()
	log.Noticef("Eevee is starting up")

	kafkaConfig := eevee.KafkaConnectionConfig{
		Server:   "localhost:9092",
		Topics:   topics,
		ClientID: "eevee",
	}
	kafkaConnection, err := eevee.NewKafkaConnection(kafkaConfig)
	if err != nil {
		log.Fatalf("Could not create Kafka Client with err: %s", err.Error())
	}

	mqttConfig := eevee.MqttConnectionConfig{
		Server:   "tcp://localhost:1883",
		Topics:   topics,
		ClientID: "eevee",
		Qos:      byte(1),
	}
	mqttConnetion, err := eevee.NewMqttConnection(mqttConfig)
	if err != nil {
		log.Fatalf("Could not create MQTT Client with err: %s", err.Error())
	}

	jsonTranslator := NewJsonTranslator()
	mapStore := NewMapStore()

	kafkaConnector := &eevee.Connector{
		Connection: kafkaConnection,
		Translator: jsonTranslator,
		IDStore:    mapStore,
	}

	mqttConnector := &eevee.Connector{
		Connection: mqttConnetion,
		Translator: jsonTranslator,
		IDStore:    mapStore,
	}

	bridge := eevee.NewBidiBridge(kafkaConnector, mqttConnector)

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
