package main

import "context"

// BidiBridge is a bi-directional bridge between two connections
type BidiBridge struct {
	c1 Connection
	c2 Connection
}

// NewBidiBridge returns an object that sends messages between these two connections based on the topics
func NewBidiBridge(c1 Connection, c2 Connection) *BidiBridge {
	return &BidiBridge{
		c1: c1,
		c2: c2,
	}
}

// Start begins the messsage exchange
func (bb *BidiBridge) Start(ctx context.Context) {
	go route(ctx, bb.c1, bb.c2)
	go route(ctx, bb.c2, bb.c1)
	bb.c1.Start(ctx)
	bb.c2.Start(ctx)
	log.Info("BidiBridge has started")
}

func route(ctx context.Context, cIn Connection, cOut Connection) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case payload := <-cIn.In():
			if !cIn.IsDuplicate(payload) {
				send(cOut, payload)
			} else {
				cIn.UnmarkPayload(payload)
			}
		}
	}
}

func send(c Connection, p Payload) {
	p = c.GenerateID(p)
	c.MarkPayload(p)
	c.Out() <- p
}
