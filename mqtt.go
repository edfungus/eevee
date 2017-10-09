package eevee

import (
	"context"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MqttConnectionConfig configures the connection to MQTT and defines what topics are accessed
type MqttConnectionConfig struct {
	Server   string
	Topics   []string
	ClientID string
	Qos      byte
}

// MqttConnection manages and abstracts the connection Kafka
type MqttConnection struct {
	client mqtt.Client
	config MqttConnectionConfig
	in     chan Payload
	out    chan Payload
}

// NewMqttConnection returns a new object connected to MQTT with specific topics
func NewMqttConnection(config MqttConnectionConfig) (*MqttConnection, error) {
	options := mqtt.NewClientOptions().SetClientID(config.ClientID).AddBroker(config.Server)
	client := mqtt.NewClient(options)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return &MqttConnection{
		client: client,
		config: config,
		in:     make(chan Payload),
		out:    make(chan Payload),
	}, nil
}

// Start begins sending and receiving MQTT messages. The context is used to stop the MQTT client
func (mc *MqttConnection) Start(ctx context.Context) {
	receiveMessages := func(client mqtt.Client, message mqtt.Message) {
		log.Debug("MQTT received message")
		mc.in <- NewPayload(message.Payload(), message.Topic())
	}
	topics := addQOSToTopics(mc.config.Topics, mc.config.Qos)
	mc.client.SubscribeMultiple(topics, receiveMessages)

	go mc.stop(ctx)
	go mc.sendMessages(ctx)
	log.Info("MQTT client has started")
}

// In receives the messages subscribed from MQTT
func (mc *MqttConnection) In() <-chan Payload {
	return mc.in
}

// Out sends messages out to MQTT
func (mc *MqttConnection) Out() chan<- Payload {
	return mc.out
}

func (mc *MqttConnection) sendMessages(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case payload := <-mc.out:
			log.Debug("MQTT sending message")
			mc.client.Publish(payload.Topic, mc.config.Qos, false, payload.RawMessage)
		}
	}
}

func (mc *MqttConnection) stop(ctx context.Context) {
	<-ctx.Done()
	log.Info("Stopping MQTT Client")
	mc.client.Disconnect(100)
	close(mc.in)
	close(mc.out)
}

func addQOSToTopics(topics []string, qos byte) map[string]byte {
	m := map[string]byte{}
	for _, topic := range topics {
		m[topic] = qos
	}
	return m
}
