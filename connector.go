package eevee

import "context"

//Connector contains all the information for this connection to interact with another connector.
//
// Notes:
// The logic in Translator will be specific to this connector as the input and the other connector as the output.
// The MessageID in Translate and IDStore will need to be the same type
type Connector struct {
	Connection Connection
	Translator Translator
	IDStore    IDStore
}

// Connection contains the actual connection made to the sink/source
type Connection interface {
	Start(ctx context.Context)
	In() <-chan Payload
	Out() chan<- Payload
	RouteStatus() chan<- RouteStatus
}

// Translator translates a raw message to the other connector's raw message
//
// Notes:
// 'out' refers to the other Connection's rawMessage format
// In most case, the translator should be shared since message conversion is not optimal
type Translator interface {
	GetID(rawMessage []byte) (messageID MessageID, err error)
	SetID(rawMessage []byte, messageID MessageID) ([]byte, error)
	TranslateOut(rawMessage []byte) (outRawMessage []byte)
}

// IDStore ensures that message are not duplicated by keeping tracking which messages have been seen
type IDStore interface {
	GenerateID() MessageID
	MarkID(id MessageID)
	UnmarkID(id MessageID)
	IsDuplicate(id MessageID) bool
}

// MessageID is how duplicate messages are identified.
//
// Notes:
// Usually the type should be same between Connectors but if they are different, the Translator and IDStore must be able to handle the translation and storage differences.
type MessageID interface{}

// NoMessageID is use to signal that there is no message id set in message
var NoMessageID = 0

// NewConnector returns a new connection object to a sink/source
func NewConnector(connection Connection, translator Translator, idStore IDStore) *Connector {
	return &Connector{
		Connection: connection,
		Translator: translator,
		IDStore:    idStore,
	}
}
