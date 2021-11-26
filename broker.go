package bee

import (
	"context"
	"sync"
)

type IBroker interface {
	Register(name string, handler Handler, opts ...Option)
	Start(ctx context.Context) error

	Send(ctx context.Context, name string, data interface{}) error
}

func NewBroker(mq MQ) IBroker {
	return &Broker{}
}

type Broker struct {
	mutex  sync.Mutex
	mq     MQ
	router map[string]Handler
	codec  Codec
}

func (b *Broker) Send(ctx context.Context, name string, body interface{}) error {
	data, err := b.codec.Encode(name, body)
	if err != nil {
		return err
	}
	return b.mq.Send(ctx, data)
}

func (b *Broker) Register(name string, handler Handler, opts ...Option) {
	b.mutex.Lock()
	//runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()
	b.router[name] = handler
	b.mutex.Unlock()
	b.codec = &LNBCodec{}
}

func (b *Broker) Start(ctx context.Context) error {
	go func() {
		for {
			data, err := b.mq.Receive(ctx)
			if err != nil {
				panic(err)
			}
			name, body := b.codec.Decode(data)
			h, ok := b.router[name]
			if !ok {
				continue
			}
			if err := h(NewContext(body)); err != nil {

			}
		}
	}()
	return nil
}
