package broker

import (
	"context"
	"github.com/fanjindong/bee"
	"github.com/fanjindong/bee/codec"
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
	codec  codec.Codec
	mws    []middleware.Middleware
}

func newBroker() *Broker {
	return &Broker{
		router: make(map[string]bee.Handler),
		codec:  codec.LNBCodec{},
		mws:    nil,
	}
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

func (b *Broker) Close() error {
	return nil
}

func (b *Broker) handler(ctx context.Context, data []byte) error {
	name, body := b.codec.Decode(data)
	handler, ok := b.router[name]
	if !ok {
		return nil
	}
	if err := handler(bee.NewContext(ctx, name, body)); err != nil {
		return err
	}
	return nil
}

func (b *Broker) Send(ctx context.Context, name string, body interface{}) error {
	data, err := b.codec.Encode(name, body)
	if err != nil {
		return err
	}
	return b.handler(ctx, data)
}

func (b *Broker) SendDelay(ctx context.Context, name string, body interface{}, delay time.Duration) error {
	data, err := b.codec.Encode(name, body)
	if err != nil {
		return err
	}
	time.AfterFunc(delay, func() { _ = b.handler(ctx, data) })
	return nil
}
