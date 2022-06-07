package amqp

import (
	"context"
	"crypto/tls"
	"github.com/bee-org/bee"
	"github.com/bee-org/bee/broker"
	"github.com/bee-org/bee/codec"
	"github.com/bee-org/bee/log"
	"github.com/bee-org/bee/message"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"math"
	"runtime"
	"sync"
	"time"
)

// Config is used in DialConfig and Open to specify the desired tuning
// parameters used during a connection open handshake.  The negotiated tuning
// will be stored in the returned connection's Config field.
type Config struct {
	// URL This specification defines an "amqp" URI scheme.
	// example amqp://user:pass@host:5672/vhost
	// The "amqp" URI scheme: https://www.rabbitmq.com/uri-spec.html
	URL string
	// TLSClientConfig specifies the client configuration of the TLS connection
	// when establishing a tls transport.
	// If the URL uses an amqp scheme, then an empty tls.Config with the
	// ServerName from the URL is used.
	TLSClientConfig *tls.Config
	// Queue name
	Queue string
	// Exchange default queue name
	Exchange string
	// ExchangeType default "direct".
	// The common types are "direct", "fanout", "topic" and "headers".
	ExchangeType string
	// RoutingKey default queue name
	RoutingKey string
	// When reconnecting to the server after connection failure, default 5s
	ReconnectDelay time.Duration
	// When setting up the channel after a channel exception, default 2s
	ReInitDelay time.Duration
	// When resending messages the server didn't confirm, default 5s
	ResendDelay time.Duration
	// Whether to enable "RabbitMQ Delayed Message Plugin"
	// When enabled, delayed messages will be delivered by plug-in
	// Need RabbitMQ Enabling the Plugin: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
	DelayedMessagePlugin bool
	// The maximum number of times the message is re-consumed. default 16 times.
	RetryMaxReconsume uint8
	//The Duration of backoff to apply between retries. default 2^retry*100ms
	RetryBackoff func(retry uint8) time.Duration
	// Custom codec
	Codec codec.Codec
	// Define the concurrency number of worker processes, default runtime.NumCPU()*2
	Concurrency int
	// A Logger represents an active logging object that generates lines of output to an io.Writer
	Logger log.ILogger
}

type Broker struct {
	*broker.Broker
	config *Config

	session *Session
}

var defaultRetryBackoff = func(retry uint8) time.Duration {
	return time.Duration(math.Pow(2, float64(retry))*100) * time.Millisecond
}

func NewBroker(config Config) (broker.IBroker, error) {
	_, err := amqp.ParseURI(config.URL)
	if err != nil {
		return nil, err
	}
	if config.Queue == "" {
		return nil, errors.New("config.Queue is empty")
	}
	if config.Exchange == "" {
		config.Exchange = config.Queue
	}
	if config.ExchangeType == "" {
		config.ExchangeType = "direct"
	}
	if config.RoutingKey == "" {
		config.RoutingKey = config.Queue
	}
	if config.Codec == nil {
		config.Codec = &codec.VNDCodec{}
	}
	if config.RetryBackoff == nil {
		config.RetryBackoff = defaultRetryBackoff
	}
	if config.RetryMaxReconsume < 1 {
		config.RetryMaxReconsume = 16
	}
	if config.Logger == nil {
		config.Logger = log.NewDefaultLogger().SetLevel(log.InfoLevel)
	}
	return &Broker{
		Broker:  broker.NewBroker(),
		config:  &config,
		session: NewSession(&config)}, nil
}

func (b *Broker) Worker() error {
	_ = b.Broker.Worker()
	if b.config.Concurrency < 1 {
		b.config.Concurrency = runtime.NumCPU() * 2
	}
	b.watch(b.Ctx())
	return nil
}

func (b *Broker) Send(ctx context.Context, name string, value interface{}) error {
	data, err := b.config.Codec.Encode(message.NewMsg(name, value))
	if err != nil {
		b.config.Logger.Errorf("Send(name=%s, value=%v), error: %v", name, value, err)
		return err
	}
	err = b.session.Push(ctx, data, 0)
	if err != nil {
		b.config.Logger.Errorf("Send(name=%s, value=%v), error: %v", name, value, err)
	}
	return err
}

func (b *Broker) SendDelay(ctx context.Context, name string, value interface{}, delay time.Duration) error {
	if delay == 0 {
		return b.Send(ctx, name, value)
	}
	data, err := b.config.Codec.Encode(message.NewMsg(name, value))
	if err != nil {
		b.config.Logger.Errorf("SendDelay(name=%s, value=%v, delay=%v), error: %v", name, value, delay.String(), err)
		return err
	}
	err = b.session.Push(ctx, data, delay.Milliseconds())
	if err != nil {
		b.config.Logger.Errorf("SendDelay(name=%s, value=%v, delay=%v), error: %v", name, value, delay.String(), err)
	}
	return err
}

func (b *Broker) Close() error {
	_ = b.session.Close()
	return b.Broker.Close()
}

func (b *Broker) watch(ctx context.Context) {
	// watch topic,topic:Delay, write to the buffer
	consumer := NewConsumer(b.session)
	buffer := consumer.GetBuffer()

	go func() {
		seat := make(chan struct{}, b.config.Concurrency)
		defer close(seat)
		for i := 0; i < b.config.Concurrency; i++ {
			seat <- struct{}{}
		}
		wg := sync.WaitGroup{}
		for {
			select {
			case data, open := <-buffer:
				if !open {
					wg.Wait()
					b.Finish()
					return
				}
				<-seat
				wg.Add(1)
				go func() {
					_ = b.process(ctx, data.Body)
					_ = data.Ack(false)
					wg.Done()
					seat <- struct{}{}
				}()
			}
		}
	}()
}

func (b *Broker) process(ctx context.Context, data []byte) error {
	msg, err := b.config.Codec.Decode(data)
	if err != nil {
		b.config.Logger.Errorf("process unknown data: %s", err)
		return err
	}
	handler, ok := b.Router(msg.GetName())
	if !ok {
		b.config.Logger.Warningf("process unknown name: %s", msg.GetName())
		return nil
	}
	if err := handler(bee.NewCtx(ctx, msg)); err != nil {
		_ = b.sendRetryQueue(msg)
		return err
	}
	return nil
}

func (b *Broker) sendRetryQueue(msg message.Message) error {
	rc := msg.IncrRetryCount()
	if rc >= b.config.RetryMaxReconsume {
		return nil
	}
	data, err := b.config.Codec.Encode(msg)
	if err != nil {
		return err
	}
	err = b.session.Push(context.Background(), data, b.config.RetryBackoff(rc-1).Milliseconds())
	if err != nil {
		b.config.Logger.Errorf("sendRetryQueue(RetryCount=%v, Name=%s, body=%v), error: %v", rc, msg.GetName(), msg.GetBody(), err)
	}
	return err
}
