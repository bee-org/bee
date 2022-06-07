package broker

import (
	"context"
	"github.com/bee-org/bee"
	"github.com/bee-org/bee/middleware"
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
	Send(ctx context.Context, name string, value interface{}) error
	// SendDelay Produce the delay message to execute the handler function asynchronously
	SendDelay(ctx context.Context, name string, body interface{}, delay time.Duration) error
}

type Broker struct {
	router   map[string]bee.Handler
	mws      []middleware.Middleware
	ctx      context.Context
	cancel   context.CancelFunc
	finished chan struct{}
}

func NewBroker() *Broker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Broker{router: make(map[string]bee.Handler),
		ctx: ctx, cancel: cancel, finished: make(chan struct{})}
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

// Close Block waiting for method Finish to call
func (b *Broker) Close() error {
	b.cancel() // just starts the close
	// Wait for all goroutine to close
	<-b.finished // close has been completed
	return nil
}

func (b *Broker) Send(ctx context.Context, name string, data interface{}) error {
	panic("implement me")
}

func (b *Broker) SendDelay(ctx context.Context, name string, body interface{}, delay time.Duration) error {
	panic("implement me")
}
func (b *Broker) Ctx() context.Context {
	return b.ctx
}
func (b *Broker) Cancel() {
	b.cancel()
}
func (b *Broker) Finish() {
	close(b.finished)
}
