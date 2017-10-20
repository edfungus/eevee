package main

import (
	"math/rand"

	"github.com/edfungus/eevee"
)

// mapStore stores the keys in a map. mapStore uses int as the key
type mapStore struct {
	store map[int]bool
}

const (
	min = 1
	max = 65535
)

func NewMapStore() *mapStore {
	return &mapStore{
		store: make(map[int]bool),
	}
}

func (ms *mapStore) GenerateID() eevee.MessageID {
	return rand.Intn(max-min) + min // Collisions can happen here btw
}

func (ms *mapStore) MarkID(id eevee.MessageID) {
	ms.store[id.(int)] = true
}

func (ms *mapStore) UnmarkID(id eevee.MessageID) {
	ms.store[id.(int)] = false
}

func (ms *mapStore) IsDuplicate(id eevee.MessageID) bool {
	return ms.store[id.(int)]
}
