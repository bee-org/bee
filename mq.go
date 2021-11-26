package bee

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

type MQ interface {
	Send(ctx context.Context, body []byte) error
	Receive(ctx context.Context) ([]byte, error)
}

type RocketMQConfig struct {
	Hosts        []string
	Topic        string
	GroupName    string
	Order        bool
	BroadCasting bool
}

func NewRocketMQ(config RocketMQConfig) (MQ, error) {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer(config.Hosts),
		consumer.WithGroupName(config.GroupName),
		consumer.WithConsumerOrder(config.Order),
		consumer.WithConsumerModel(func() consumer.MessageModel {
			if config.BroadCasting {
				return consumer.BroadCasting
			}
			return consumer.Clustering
		}()),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset))
	if err != nil {
		return nil, err
	}
	r := &Rocket{consumer: c, buffer: make(chan []byte)}
	if err = c.Subscribe(config.Topic, consumer.MessageSelector{}, newConsumerHandler(r)); err != nil {
		return nil, err
	}
	if err = c.Start(); err != nil {
		return nil, err
	}
	return r, nil
}

type Rocket struct {
	producer rocketmq.Producer
	consumer rocketmq.PushConsumer
	buffer   chan []byte
}

func (r Rocket) Send(ctx context.Context, body []byte) error {
	panic("implement me")
}

func (r Rocket) Receive(ctx context.Context) ([]byte, error) {
	return <-r.buffer, nil
}

func (r *Rocket) writeBuffer(body []byte) {
	r.buffer <- body
}

func newConsumerHandler(c *Rocket) func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	return func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			c.writeBuffer(msg.Body)
		}
		return consumer.ConsumeSuccess, nil
	}
}
