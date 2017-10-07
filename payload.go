package main

type Payload struct {
	ID      int
	Message []byte // maybe make interface{}?
	Topic   string
}

// RawMessageHandler handles serializing and deserializing payload to extract message id
type RawMessageHandler interface {
	GetID(rawMessage []byte) int
	GetMessage(rawMessage []byte) []byte
	NewRawMessage(id int, message []byte) []byte
}

// IDStore ensures that message are not duplicated by keeping tracking which messages have been seen
type IDStore interface {
	GenerateID(payload Payload) int
	MarkID(id int)
	UnmarkID(id int)
	IsDuplicate(id int) bool
}
