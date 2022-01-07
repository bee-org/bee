package broker

import (
	"context"
	"github.com/fanjindong/bee"
	"github.com/fanjindong/bee/middleware"
	"time"
)

type IBroker interface {
	// Register Registration handler function
	Register(name string, handler bee.Handler, opts ...bee.Option)
	// Middleware Registers the middleware of the handler function
	Middleware(mws ...middleware.Middleware)
	// Worker Start consumer workers
	Worker() error
	// Close broker
	Close() error

	// Send Produce the message to execute the handler function asynchronously
	Send(ctx context.Context, name string, data interface{}) error
	// SendDelay Produce the delay message to execute the handler function asynchronously
	SendDelay(ctx context.Context, name string, body interface{}, delay time.Duration) error
}
