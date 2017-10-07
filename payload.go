package main

type Payload struct {
	ID      int
	Message []byte
	Topic   string
}

// IDResolver handles serializing and deserializing payload to extract message id
type IDResolver interface {
	GetID(rawMessage []byte) int
	SetID(rawMessage []byte, id int) []byte
}

// IDStore ensures that message are not duplicated by keeping tracking which messages have been seen
type IDStore interface {
	GenerateID(payload Payload) int
	MarkID(id int)
	UnmarkID(id int)
	IsDuplicate(id int) bool
}
