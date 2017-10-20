package main

import (
	"github.com/edfungus/eevee"
)

// dumbStore says all messages haven't been processed
type dumbStore struct {
}

func NewDumbStore() *dumbStore {
	return &dumbStore{}
}

func (ds *dumbStore) GenerateID() eevee.MessageID {
	return 1
}

func (ds *dumbStore) MarkID(id eevee.MessageID) {
}

func (ds *dumbStore) UnmarkID(id eevee.MessageID) {
}

func (ds *dumbStore) IsDuplicate(id eevee.MessageID) bool {
	return false
}
