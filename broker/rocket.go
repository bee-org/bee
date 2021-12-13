package broker

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/fanjindong/bee"
	"github.com/fanjindong/bee/codec"
	"github.com/fanjindong/bee/middleware"
	"sync"
)

type RocketMQConfig struct {
	Hosts             []string
	Topic             string
	ProducerGroupName string
	ConsumerGroupName string
	Order             bool
	BroadCasting      bool
	Codec             codec.Codec
}

func NewRocketMQBroker(config RocketMQConfig) (IBroker, error) {
	b := &RocketMQBroker{codec: &codec.LNBCodec{}, topic: config.Topic, router: map[string]bee.Handler{}}
	if config.Codec != nil {
		b.codec = config.Codec
	}

	if config.ConsumerGroupName != "" {
		c, err := rocketmq.NewPushConsumer(
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
			consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset))
		if err != nil {
			return nil, err
		}
		b.consumer = c
	}
	if config.ProducerGroupName != "" {
		p, err := rocketmq.NewProducer(
			producer.WithNameServer(config.Hosts),
			producer.WithRetry(3),
			producer.WithGroupName(config.ProducerGroupName),
		)
		if err != nil {
			return nil, err
		}
		b.producer = p
	}
	return b, nil
}

type RocketMQBroker struct {
	mutex    sync.Mutex
	topic    string
	producer rocketmq.Producer
	consumer rocketmq.PushConsumer
	router   map[string]bee.Handler
	codec    codec.Codec
	mws      []middleware.Middleware
}

func (b *RocketMQBroker) Send(ctx context.Context, name string, body interface{}) error {
	data, err := b.codec.Encode(name, body)
	if err != nil {
		return err
	}
	_, err = b.producer.SendSync(ctx, &primitive.Message{Topic: b.topic, Body: data})
	return err
}

func (b *RocketMQBroker) Register(name string, handler bee.Handler, opts ...bee.Option) {
	b.mutex.Lock()
	//runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()
	b.router[name] = handler
	b.mutex.Unlock()
}

func (b *RocketMQBroker) Middleware(mws ...middleware.Middleware) {
	b.mws = append(b.mws, mws...)
}

func (b *RocketMQBroker) Start() error {
	for _, mw := range b.mws {
		for name, handler := range b.router {
			b.router[name] = mw(handler)
		}
	}
	if b.producer != nil {
		if err := b.producer.Start(); err != nil {
			return err
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

func (b *RocketMQBroker) Close() error {
	b.producer.Shutdown()
	return b.consumer.Shutdown()
}

func (b *RocketMQBroker) handler(ctx context.Context, data []byte) error {
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

func newConsumerHandler(b *RocketMQBroker) func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	return func(ctx context.Context, mes ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, me := range mes {
			if err := b.handler(ctx, me.Body); err != nil {
				return consumer.ConsumeRetryLater, err
			}
		}
		return consumer.ConsumeSuccess, nil
	}
}
