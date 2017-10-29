package eevee

import (
	"context"
)

// BidiBridge is a bi-directional bridge between two connections
type BidiBridge struct {
	c1 *Connector
	c2 *Connector
}

// NewBidiBridge returns an object that sends messages between these two connections based on the topics
func NewBidiBridge(c1 *Connector, c2 *Connector) *BidiBridge {
	return &BidiBridge{
		c1: c1,
		c2: c2,
	}
}

// Start begins the messsage exchange
func (bb *BidiBridge) Start(ctx context.Context) {
	go route(ctx, bb.c1, bb.c2)
	go route(ctx, bb.c2, bb.c1)
	bb.c1.Connection.Start(ctx)
	bb.c2.Connection.Start(ctx)
	log.Info("BidiBridge has started")
}

// UniBridge is a uni-directional bridge from one connection to another connection
type UniBridge struct {
	in  *Connector
	out *Connector
}

// NewUniBridge returns an object that sends messages from in to out
func NewUniBridge(in *Connector, out *Connector) *UniBridge {
	return &UniBridge{
		in:  in,
		out: out,
	}
}

// Start begins the messsage transmission
func (ub *UniBridge) Start(ctx context.Context) {
	go route(ctx, ub.in, ub.out)
	go dumpIncoming(ctx, ub.out)
	ub.in.Connection.Start(ctx)
	ub.out.Connection.Start(ctx)
	log.Info("UniBridge has started")
}

func route(ctx context.Context, cIn *Connector, cOut *Connector) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case inPayload := <-cIn.Connection.In():
			id, err := cIn.Translator.GetID(inPayload.RawMessage)
			if err != nil {
				msg := "Message receive but could not get id. Skipping"
				log.Debug(msg)
				cIn.Connection.RouteStatus() <- RouteStatus{
					Code:    RouteFailed,
					Message: msg,
				}
				continue
			}
			if !cIn.IDStore.IsDuplicate(id) {
				outRawMessage := cIn.Translator.TranslateOut(inPayload.RawMessage)
				if id == NoMessageID {
					id = cOut.IDStore.GenerateID()
					outRawMessage, err = cOut.Translator.SetID(outRawMessage, id)
					if err != nil {
						msg := "Could not set id in outgoing payload"
						log.Debug()
						cIn.Connection.RouteStatus() <- RouteStatus{
							Code:    RouteFailed,
							Message: msg,
						}
						continue
					}
				}
				cOut.IDStore.MarkID(id)
				outPayload := NewPayload(outRawMessage, inPayload.Topic)
				cOut.Connection.Out() <- outPayload
				cIn.Connection.RouteStatus() <- RouteStatus{
					Code: RouteOK,
				}
			} else {
				cIn.IDStore.UnmarkID(id)
			}
		}
	}
}

func dumpIncoming(ctx context.Context, c *Connector) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-c.Connection.In():
			// read channel but do nothing
		}
	}
}
