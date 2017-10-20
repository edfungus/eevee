package main

import (
	"encoding/json"

	"github.com/edfungus/eevee"
)

// messagePayload is the expected json payload structure for the input and output
type messagePayload struct {
	ID int `json:"id"`
}

// JsonTranslator in/outputs same json with known structure
type JsonTranslator struct{}

func NewJsonTranslator() *JsonTranslator {
	return &JsonTranslator{}
}

func (jt *JsonTranslator) GetID(rawMessage []byte) (eevee.MessageID, error) {
	var message messagePayload
	err := json.Unmarshal(rawMessage, &message)
	if err != nil {
		return nil, err
	}
	if message.ID == 0 {
		return eevee.NoMessageID, nil
	}
	return message.ID, nil
}

func (jt *JsonTranslator) SetID(rawMessage []byte, id eevee.MessageID) ([]byte, error) {
	var message messagePayload
	err := json.Unmarshal(rawMessage, &message)
	if err != nil {
		return nil, err
	}
	message.ID = id.(int)
	return json.Marshal(message)
}

func (jt *JsonTranslator) TranslateOut(rawMessage []byte) (outRawMessage []byte) {
	return rawMessage
}
