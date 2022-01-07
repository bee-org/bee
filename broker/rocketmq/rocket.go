package rocketmq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/fanjindong/bee"
	"github.com/fanjindong/bee/broker"
	"github.com/fanjindong/bee/codec"
	"github.com/fanjindong/bee/middleware"
	"sync"
	"time"
)

type Config struct {
	Hosts             []string
	Topic             string
	ProducerGroupName string
	ConsumerGroupName string
	Order             bool
	BroadCasting      bool
	// default processId, warning: using defaults can be problematic when docker is deployed
	InstanceName string
	// Strategy Algorithm for message allocating between consumers. reference: https://github.com/apache/rocketmq-client-go/blob/master/consumer/strategy.go
	AllocateStrategy func(string, string, []*primitive.MessageQueue, []string) []*primitive.MessageQueue
	// The maximum number of times a message should be retried. default 16 times.
	MaxReconsumeTimes int
	// The Duration of backoff to apply between retries.
	//Backoff time.Duration
	Codec codec.Codec
}

func NewBroker(config Config) (broker.IBroker, error) {
	rlog.SetLogLevel("error")
	b := &Broker{codec: &codec.LNBCodec{}, topic: config.Topic, router: map[string]bee.Handler{}}
	if config.Codec != nil {
		b.codec = config.Codec
	}

	if config.ConsumerGroupName != "" {
		opts := []consumer.Option{
			consumer.WithNameServer(config.Hosts),
			consumer.WithGroupName(config.ConsumerGroupName),
			consumer.WithConsumerOrder(config.Order),
			consumer.WithConsumerModel(func() consumer.MessageModel {
				if config.BroadCasting {
					return consumer.BroadCasting
				}
				return consumer.Clustering
			}()),
			consumer.WithConsumeMessageBatchMaxSize(1),
			consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset),
		}
		if config.InstanceName != "" {
			opts = append(opts, consumer.WithInstance(config.InstanceName))
		}
		if config.AllocateStrategy != nil {
			opts = append(opts, consumer.WithStrategy(config.AllocateStrategy))
		}
		if config.MaxReconsumeTimes > 0 {
			opts = append(opts, consumer.WithMaxReconsumeTimes(int32(config.MaxReconsumeTimes)))
		}
		c, err := rocketmq.NewPushConsumer(opts...)
		if err != nil {
			return nil, err
		}
		b.consumer = c
	}
	if config.ProducerGroupName != "" {
		opts := []producer.Option{
			producer.WithNameServer(config.Hosts),
			producer.WithRetry(3),
			producer.WithGroupName(config.ProducerGroupName),
		}
		if config.InstanceName != "" {
			opts = append(opts, producer.WithInstanceName(config.InstanceName))
		}
		p, err := rocketmq.NewProducer(opts...)
		if err != nil {
			return nil, err
		}
		if err = p.Start(); err != nil {
			return nil, err
		}
		b.producer = p
	}
	return b, nil
}

type Broker struct {
	mutex    sync.Mutex
	topic    string
	producer rocketmq.Producer
	consumer rocketmq.PushConsumer
	router   map[string]bee.Handler
	codec    codec.Codec
	mws      []middleware.Middleware
}

func (b *Broker) Register(name string, handler bee.Handler, opts ...bee.Option) {
	b.mutex.Lock()
	//runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()
	b.router[name] = handler
	b.mutex.Unlock()
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
	if b.consumer != nil {
		if err := b.consumer.Subscribe(b.topic, consumer.MessageSelector{}, newConsumerHandler(b)); err != nil {
			return err
		}
		if err := b.consumer.Start(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) Close() error {
	_ = b.producer.Shutdown()
	return b.consumer.Shutdown()
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

func newConsumerHandler(b *Broker) func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	return func(ctx context.Context, mes ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, me := range mes {
			if err := b.handler(ctx, me.Body); err != nil {
				return consumer.ConsumeRetryLater, err
			}
		}
		return consumer.ConsumeSuccess, nil
	}
}

func (b *Broker) Send(ctx context.Context, name string, body interface{}) error {
	data, err := b.codec.Encode(name, body)
	if err != nil {
		return err
	}
	msg := primitive.NewMessage(b.topic, data)
	_, err = b.producer.SendSync(ctx, msg)
	return err
}

func (b *Broker) SendDelay(ctx context.Context, name string, body interface{}, delay time.Duration) error {
	if delay == 0 {
		return b.Send(ctx, name, body)
	}
	data, err := b.codec.Encode(name, body)
	if err != nil {
		return err
	}
	msg := primitive.NewMessage(b.topic, data).WithDelayTimeLevel(duration2DelayTimeLevel(delay))
	_, err = b.producer.SendSync(ctx, msg)
	return err
}

func duration2DelayTimeLevel(d time.Duration) int {
	s := d.Seconds()
	// reference delay level definition: 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
	switch {
	case s <= 1:
		return 1
	case s <= 5:
		return 2
	case s <= 10:
		return 3
	case s <= 30:
		return 4
	case s <= 60:
		return 5
	case s <= 60*2:
		return 6
	case s <= 60*3:
		return 7
	case s <= 60*4:
		return 8
	case s <= 60*5:
		return 9
	case s <= 60*6:
		return 10
	case s <= 60*7:
		return 11
	case s <= 60*8:
		return 12
	case s <= 60*9:
		return 13
	case s <= 60*10:
		return 14
	case s <= 60*20:
		return 15
	case s <= 60*30:
		return 16
	case s <= 60*60:
		return 17
	default:
		return 18
	}
}
