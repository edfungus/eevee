package main

import (
	"github.com/edfungus/eevee"
)

// dumbTranslator takes all messages and doesn't care for duplicates
type dumbTranslator struct{}

func NewDumbTranslator() *dumbTranslator {
	return &dumbTranslator{}
}

func (dt *dumbTranslator) GetID(rawMessage []byte) (eevee.MessageID, error) {
	return 1, nil
}

func (dt *dumbTranslator) SetID(rawMessage []byte, id eevee.MessageID) ([]byte, error) {
	return []byte{}, nil
}

func (dt *dumbTranslator) TranslateOut(rawMessage []byte) (outRawMessage []byte) {
	return []byte("{}")
}
