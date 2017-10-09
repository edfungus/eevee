package eevee

import "context"

type Connection interface {
	Start(ctx context.Context)
	In() <-chan Payload
	Out() chan<- Payload
}
