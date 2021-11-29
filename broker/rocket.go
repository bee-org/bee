package broker

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/fanjindong/bee"
	"sync"
)

type RocketMQConfig struct {
	Hosts             []string
	Topic             string
	ProducerGroupName string
	ConsumerGroupName string
	Order             bool
	BroadCasting      bool
}

func NewRocketMQBroker(config RocketMQConfig) (IBroker, error) {
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
	p, err := rocketmq.NewProducer(
		producer.WithNameServer(config.Hosts),
		producer.WithRetry(3),
		producer.WithGroupName(config.ProducerGroupName),
	)
	b := &RocketMQBroker{producer: p, consumer: c, codec: &bee.LNBCodec{}, topic: config.Topic, router: map[string]bee.Handler{}}
	if err = c.Subscribe(config.Topic, consumer.MessageSelector{}, newConsumerHandler(b)); err != nil {
		return nil, err
	}
	return b, nil
}

type RocketMQBroker struct {
	mutex    sync.Mutex
	topic    string
	producer rocketmq.Producer
	consumer rocketmq.PushConsumer
	router   map[string]bee.Handler
	codec    bee.Codec
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
	b.codec = &bee.LNBCodec{}
}

func (b *RocketMQBroker) Start() error {
	if err := b.consumer.Start(); err != nil {
		return err
	}
	if err := b.producer.Start(); err != nil {
		return err
	}
	return nil
}

func (b *RocketMQBroker) Close() error {
	_ = b.producer.Shutdown()
	return b.consumer.Shutdown()
}

func (b *RocketMQBroker) handler(ctx context.Context, data []byte) error {
	name, body := b.codec.Decode(data)
	handler, ok := b.router[name]
	if !ok {
		return nil
	}
	if err := handler(bee.NewContext(ctx, body)); err != nil {
		return err
	}
	return nil
}

func newConsumerHandler(b *RocketMQBroker) func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	return func(ctx context.Context, mes ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, me := range mes {
			fmt.Println(me.String())
			if err := b.handler(ctx, me.Body); err != nil {
				return consumer.ConsumeRetryLater, err
			}
		}
		return consumer.ConsumeSuccess, nil
	}
}
