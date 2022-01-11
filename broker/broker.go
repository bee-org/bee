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

type Broker struct {
	router map[string]bee.Handler
	mws    []middleware.Middleware
}

func NewBroker() *Broker {
	return &Broker{router: make(map[string]bee.Handler)}
}

func (b *Broker) Register(name string, handler bee.Handler, opts ...bee.Option) {
	b.router[name] = handler
}

func (b *Broker) Middleware(mws ...middleware.Middleware) {
	b.mws = append(b.mws, mws...)
}

func (b *Broker) Worker() error {
	for _, mw := range b.mws {
		for name, handler := range b.router {
			b.router[name] = mw(handler)
		}
	}
	return nil
}

func (b *Broker) Router(name string) (handler bee.Handler, ok bool) {
	handler, ok = b.router[name]
	return
}

func (b *Broker) Close() error {
	panic("implement me")
}

func (b *Broker) Send(ctx context.Context, name string, data interface{}) error {
	panic("implement me")
}

func (b *Broker) SendDelay(ctx context.Context, name string, body interface{}, delay time.Duration) error {
	panic("implement me")
}
