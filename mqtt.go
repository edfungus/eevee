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
	Username string
	Password string
}

// MqttConnection manages and abstracts the connection Kafka
type MqttConnection struct {
	client      mqtt.Client
	config      MqttConnectionConfig
	in          chan Payload
	out         chan Payload
	routeStatus chan RouteStatus
}

// NewMqttConnection returns a new object connected to MQTT with specific topics
func NewMqttConnection(config MqttConnectionConfig) (*MqttConnection, error) {
	options := mqtt.NewClientOptions().SetClientID(config.ClientID).AddBroker(config.Server).SetUsername(config.Username).SetPassword(config.Password)
	client := mqtt.NewClient(options)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return &MqttConnection{
		client:      client,
		config:      config,
		in:          make(chan Payload),
		out:         make(chan Payload),
		routeStatus: make(chan RouteStatus),
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
	go mc.ignoreRouteStatus(ctx)
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

// Errors from eevee
func (mc *MqttConnection) RouteStatus() chan<- RouteStatus {
	return mc.routeStatus
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

func (mc *MqttConnection) ignoreRouteStatus(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-mc.routeStatus:
		}
	}
}
