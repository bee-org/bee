package redis

import (
	"context"
	"github.com/bee-org/bee"
	"github.com/bee-org/bee/broker"
	"github.com/bee-org/bee/codec"
	"github.com/bee-org/bee/log"
	"github.com/bee-org/bee/message"
	"github.com/go-redis/redis/v8"
	"math"
	"runtime"
	"strconv"
	"sync"
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
	buffer chan []byte

	c *redis.Client
}

var defaultRetryBackoff = func(retry uint8) time.Duration {
	return time.Duration(math.Pow(2, float64(retry))*100) * time.Millisecond
}

func NewBroker(config Config) (broker.IBroker, error) {
	opt, err := redis.ParseURL(config.URL)
	if err != nil {
		return nil, err
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
		Broker: broker.NewBroker(),
		config: &config,
		c:      redis.NewClient(opt)}, nil
}

func (b *Broker) Worker() error {
	_ = b.Broker.Worker()
	if b.config.Concurrency < 1 {
		b.config.Concurrency = runtime.NumCPU() * 2
	}
	b.buffer = make(chan []byte, b.config.Concurrency)
	b.watch(b.Ctx())
	return nil
}

func (b *Broker) Send(ctx context.Context, name string, value interface{}) error {
	data, err := b.config.Codec.Encode(message.NewMsg(name, value))
	if err != nil {
		b.config.Logger.Errorf("Send(name=%s, value=%v), error: %v", name, value, err)
		return err
	}
	err = b.c.RPush(ctx, b.config.Topic, data).Err()
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
	err = b.c.ZAdd(ctx, b.getDelayTopic(), &redis.Z{Score: float64(time.Now().Add(delay).UnixNano()), Member: data}).Err()
	if err != nil {
		b.config.Logger.Errorf("SendDelay(name=%s, value=%v, delay=%v), error: %v", name, value, delay.String(), err)
	}
	return err
}

func (b *Broker) watch(ctx context.Context) {
	// watch topic,topic:Delay, write to the buffer
	closed := ctx.Done()
	delayed := make(chan struct{})
	go func() {
		for {
			select {
			case <-closed:
				close(delayed)
				return
			default:
				_ = b.getOneByQueue(ctx)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-delayed:
				close(b.buffer)
				return
			default:
				// Loop to get the delay message until it is empty, wait 100 milliseconds
				err := b.getOneByDelayQueue(ctx)
				if err == redis.Nil {
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}()

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
					_ = b.process(ctx, data)
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
		return b.sendDeadLetterQueue(msg)
	}
	data, err := b.config.Codec.Encode(msg)
	if err != nil {
		return err
	}
	score := float64(time.Now().Add(b.config.RetryBackoff(rc - 1)).UnixNano())
	err = b.c.ZAdd(context.Background(), b.getDelayTopic(), &redis.Z{Score: score, Member: data}).Err()
	if err != nil {
		b.config.Logger.Errorf("sendRetryQueue(RetryCount=%v, Name=%s, body=%v), error: %v", rc, msg.GetName(), msg.GetBody(), err)
	}
	return err
}

func (b *Broker) sendDeadLetterQueue(msg message.Message) error {
	data, err := b.config.Codec.Encode(msg)
	if err != nil {
		return err
	}
	err = b.c.RPush(context.Background(), b.getDeadLetterTopic(), data).Err()
	if err != nil {
		b.config.Logger.Errorf("sendDeadLetterQueue(RetryCount=%v, Name=%s, body=%v), error: %v", msg.GetRetryCount(), msg.GetName(), msg.GetBody(), err)
	}
	return err
}

func (b *Broker) getOneByDelayQueue(ctx context.Context) (err error) {
	var items []string
	var result []byte
	key := b.getDelayTopic()
	watchFunc := func(tx *redis.Tx) error {
		now := time.Now().UTC().UnixNano()
		// https://redis.io/commands/zrangebyscore
		items, err = tx.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
			Min: "0", Max: strconv.FormatInt(now, 10), Offset: 0, Count: 1,
		}).Result()
		if err != nil {
			return err
		} else if len(items) == 0 {
			return redis.Nil
		}
		// only return the first range value if there are no other changes in this key
		// to make sure a delayed task would only be consumed once
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZRem(ctx, key, items[0])
			result = []byte(items[0])
			return nil
		})
		return err
	}
	err = b.c.Watch(ctx, watchFunc, key)
	if err == nil && len(result) > 0 {
		b.buffer <- result
	}
	return
}

func (b *Broker) getOneByQueue(ctx context.Context) error {
	items, err := b.c.BLPop(ctx, 1*time.Second, b.config.Topic).Result()
	if err != nil {
		return err
	}
	// items[0],items[1] - key,element
	if len(items) < 2 {
		return redis.Nil
	}
	if items[1] != "" {
		b.buffer <- []byte(items[1])
	}
	return nil
}

func (b *Broker) getDelayTopic() string      { return b.config.Topic + ":Delay" }
func (b *Broker) getDeadLetterTopic() string { return b.config.Topic + ":DeadLetter" }
