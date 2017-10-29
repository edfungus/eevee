package eevee

type Payload struct {
	RawMessage []byte
	Topic      string
}

type RouteStatus struct {
	Code    int
	Message string
}

const (
	RouteOK     = 0
	RouteFailed = 1
)

// NewPayload returns a new Payload
func NewPayload(rawMessage []byte, topic string) Payload {
	return Payload{
		RawMessage: rawMessage,
		Topic:      topic,
	}
}
