package eevee

type Payload struct {
	RawMessage []byte
	Topic      string
}

// NewPayload returns a new Payload
func NewPayload(rawMessage []byte, topic string) Payload {
	return Payload{
		RawMessage: rawMessage,
		Topic:      topic,
	}
}
