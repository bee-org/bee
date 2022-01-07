package redis

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/fanjindong/bee"
	"github.com/fanjindong/bee/broker"
	"github.com/fanjindong/bee/codec"
	"github.com/fanjindong/bee/middleware"
	"github.com/go-redis/redis/v8"
	"runtime"
	"time"
)

type Config struct {
	// URL be used to connect to Redis.
	// Scheme is required.
	// There are two connection types: by tcp socket and by unix socket.
	// Tcp connection:
	//		redis://<user>:<password>@<host>:<port>/<db_number>
	// Unix connection:
	//		unix://<user>:<password>@</path/to/redis.sock>?db=<db_number>
	URL   string
	Topic string
	// The maximum number of times a message should be retried. default 16 times.
	MaxReconsumeTimes int
	// The Duration of backoff to apply between retries.
	//Backoff time.Duration
	// Custom codec
	Codec codec.Codec
	// Define the number of worker processes, default runtime.NumCPU()*2
	WorkerNumber int
}

type Broker struct {
	router map[string]bee.Handler
	codec  codec.Codec
	mws    []middleware.Middleware
	config *Config

	c *redis.Client
}

func NewBroker(config Config) (broker.IBroker, error) {
	opt, err := redis.ParseURL(config.URL)
	if err != nil {
		return nil, err
	}
	if config.Codec == nil {
		config.Codec = &codec.LNBCodec{}
	}
	return &Broker{
		router: make(map[string]bee.Handler),
		codec:  config.Codec,
		config: &config,
		c:      redis.NewClient(opt)}, nil
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
	if b.config.WorkerNumber < 1 {
		b.config.WorkerNumber = runtime.NumCPU() * 2
	}
	channel := make(chan pulsar.ConsumerMessage, b.config.WorkerNumber*2)
	opt := pulsar.ConsumerOptions{
		Topic:               b.config.Topic,
		SubscriptionName:    b.config.SubscriptionName,
		Type:                pulsar.Shared,
		ReceiverQueueSize:   b.config.ReceiverQueueSize,
		MessageChannel:      channel,
		RetryEnable:         b.config.RetryEnable,
		NackRedeliveryDelay: b.config.NackRedeliveryDelay,
	}
	if b.config.DLQ != nil {
		opt.DLQ = &pulsar.DLQPolicy{
			MaxDeliveries:    b.config.DLQ.MaxDeliveries,
			DeadLetterTopic:  b.config.DLQ.DeadLetterTopic,
			RetryLetterTopic: b.config.DLQ.RetryLetterTopic,
		}
	}
	c, err := b.client.Subscribe(opt)
	if err != nil {
		return err
	}
	b.consumer = c
	b.watch(channel)
	return nil
}

func (b *Broker) Close() error {
	if b.consumer != nil {
		b.consumer.Close()
	}
	b.producer.Close()
	b.client.Close()
	return nil
}

func (b *Broker) Send(ctx context.Context, name string, data interface{}) error {
	body, err := b.codec.Encode(name, data)
	if err != nil {
		return err
	}
	_, err = b.producer.Send(ctx, &pulsar.ProducerMessage{Payload: body})
	return err
}

func (b *Broker) SendDelay(ctx context.Context, name string, data interface{}, delay time.Duration) error {
	if delay == 0 {
		return b.Send(ctx, name, data)
	}
	body, err := b.codec.Encode(name, data)
	if err != nil {
		return err
	}
	_, err = b.producer.Send(ctx, &pulsar.ProducerMessage{Payload: body, DeliverAfter: delay})
	return err
}

func (b *Broker) watch(channel chan pulsar.ConsumerMessage) {
	// Receive messages from channel. The channel returns a struct which contains message and the consumer from where
	// the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
	// shared across multiple consumers as well
	for i := 0; i < b.config.WorkerNumber; i++ {
		go func() {
			for cm := range channel {
				msg := cm.Message
				if err := b.handler(context.Background(), msg.Payload()); err != nil {
					b.consumer.NackID(msg.ID())
					continue
				}
				b.consumer.AckID(msg.ID())
			}
		}()
	}
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
