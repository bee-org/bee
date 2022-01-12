package pulsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/bee-org/bee"
	"github.com/bee-org/bee/broker"
	"github.com/bee-org/bee/codec"
	"github.com/sirupsen/logrus"
	"runtime"
	"sync"
	"time"
)

// DLQPolicy Configuration for Dead Letter Queue consumer policy
type DLQPolicy struct {
	// Maximum number of times that a message will be delivered before being sent to the dead letter queue.
	MaxDeliveries uint32
	// Name of the topic where the failing messages will be sent.
	DeadLetterTopic string
	// Name of the topic where the retry messages will be sent.
	RetryLetterTopic string
}

type Config struct {
	// Configure the service URL for the Pulsar service.
	// This parameter is required
	URL string
	// Timeout for the establishment of a TCP connection (default: 5 seconds)
	ConnectionTimeout time.Duration
	// Set the operation timeout (default: 30 seconds)
	// Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
	// operation will be marked as failed
	OperationTimeout time.Duration
	// Max number of connections to a single broker that will kept in the pool. (Default: 1 connection)
	MaxConnectionsPerBroker int
	// Authentication provider with specified auth token
	AuthToken string

	// Topic specify the topic this producer will be publishing on.
	// Specify the topic this consumer will subscribe on.
	// Either a topic, a list of topics or a topics pattern are required when subscribing
	Topic string
	// Specify the subscription name for this consumer
	// This argument is required when subscribing
	SubscriptionName string
	// Sets the size of the consumer receive queue.
	// The consumer receive queue controls how many messages can be accumulated by the `Consumer` before the
	// application handler. Using a higher value could potentially increase the consumer
	// throughput at the expense of bigger memory utilization.
	// Default value is `1000` messages and should be good for most use cases.
	ReceiverQueueSize int
	// The delay after which to redeliver the messages that failed to be
	// processed. Default is 1min.
	NackRedeliveryDelay time.Duration
	// Auto retry send messages to default filled DLQPolicy topics
	// default RetryTopic: SubscriptionName+"-RETRY", DlqTopic: SubscriptionName+"-DLQ", MaxReconsumeTimes = 16
	RetryEnable bool
	// Custom RetryTopic,DlqTopic,MaxReconsumeTimes
	DLQ *DLQPolicy
	// Define the concurrency number of worker processes, default runtime.NumCPU()*2
	Concurrency int
	// Custom codec
	Codec codec.Codec
}

type Broker struct {
	*broker.Broker
	config *Config
	buffer chan pulsar.ConsumerMessage

	client   pulsar.Client
	producer pulsar.Producer
	consumer pulsar.Consumer
}

func NewBroker(config Config) (broker.IBroker, error) {
	logger := logrus.StandardLogger()
	logger.SetLevel(logrus.ErrorLevel)
	opt := pulsar.ClientOptions{
		URL:                     config.URL,
		ConnectionTimeout:       config.ConnectionTimeout,
		OperationTimeout:        config.OperationTimeout,
		MaxConnectionsPerBroker: config.MaxConnectionsPerBroker,
		Logger:                  log.NewLoggerWithLogrus(logger),
	}
	if config.AuthToken != "" {
		opt.Authentication = pulsar.NewAuthenticationToken(config.AuthToken)
	}
	client, err := pulsar.NewClient(opt)
	if err != nil {
		return nil, err
	}
	p, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: config.Topic,
	})
	if err != nil {
		return nil, err
	}
	if config.Codec == nil {
		config.Codec = &codec.VND{}
	}
	return &Broker{Broker: broker.NewBroker(),
		config: &config, client: client, producer: p,
	}, nil
}

func (b *Broker) Worker() error {
	_ = b.Broker.Worker()
	if b.config.Concurrency < 1 {
		b.config.Concurrency = runtime.NumCPU() * 2
	}
	b.buffer = make(chan pulsar.ConsumerMessage, b.config.Concurrency)
	opt := pulsar.ConsumerOptions{
		Topic:               b.config.Topic,
		SubscriptionName:    b.config.SubscriptionName,
		Type:                pulsar.Shared,
		ReceiverQueueSize:   b.config.ReceiverQueueSize,
		MessageChannel:      b.buffer,
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
	b.watch()
	return nil
}

func (b *Broker) Close() error {
	if b.consumer != nil {
		b.consumer.Close()
		close(b.buffer)
	}
	b.producer.Close()
	b.client.Close()
	return b.Broker.Close()
}

func (b *Broker) Send(ctx context.Context, name string, data interface{}) error {
	body, err := b.config.Codec.Encode(&codec.Header{Name: name}, data)
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
	body, err := b.config.Codec.Encode(&codec.Header{Name: name}, data)
	if err != nil {
		return err
	}
	_, err = b.producer.Send(ctx, &pulsar.ProducerMessage{Payload: body, DeliverAfter: delay})
	return err
}

func (b *Broker) watch() {
	// Receive messages from channel. The channel returns a struct which contains message and the consumer from where
	// the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
	// shared across multiple consumers as well
	go func() {
		seat := make(chan struct{}, b.config.Concurrency)
		defer close(seat)
		for i := 0; i < b.config.Concurrency; i++ {
			seat <- struct{}{}
		}
		wg := sync.WaitGroup{}
		for {
			select {
			case data, open := <-b.buffer:
				if !open {
					wg.Wait()
					b.Finish()
					return
				}
				<-seat
				wg.Add(1)
				go func() {
					defer wg.Done()
					msg := data.Message
					if err := b.handler(b.Ctx(), msg.Payload()); err != nil {
						b.consumer.NackID(msg.ID())
						return
					}
					b.consumer.AckID(msg.ID())
					seat <- struct{}{}
				}()
			}
		}
	}()
}

func (b *Broker) handler(ctx context.Context, data []byte) error {
	header, body := b.config.Codec.Decode(data)
	handler, ok := b.Router(header.Name)
	if !ok {
		return nil
	}
	if err := handler(bee.NewCtx(ctx, &header, body)); err != nil {
		return err
	}
	return nil
}
