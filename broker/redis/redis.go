package redis

import (
	"context"
	"github.com/bee-org/bee"
	"github.com/bee-org/bee/broker"
	"github.com/bee-org/bee/codec"
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
		config.Codec = &codec.VND{}
	}
	if config.RetryBackoff == nil {
		config.RetryBackoff = defaultRetryBackoff
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
	data, err := b.config.Codec.Encode(&codec.Header{Name: name}, value)
	if err != nil {
		return err
	}
	err = b.c.RPush(ctx, b.config.Topic, data).Err()
	return err
}

func (b *Broker) SendDelay(ctx context.Context, name string, value interface{}, delay time.Duration) error {
	if delay == 0 {
		return b.Send(ctx, name, value)
	}
	data, err := b.config.Codec.Encode(&codec.Header{Name: name}, value)
	if err != nil {
		return err
	}
	err = b.c.ZAdd(ctx, b.config.Topic+":DELAY", &redis.Z{Score: float64(time.Now().Add(delay).UnixNano()), Member: data}).Err()
	return err
}

func (b *Broker) watch(ctx context.Context) {
	// watch topic,topic:DELAY, write to the buffer
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
				_ = b.getOneByDelayQueue(ctx)
				time.Sleep(100 * time.Millisecond)
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
	header, body := b.config.Codec.Decode(data)
	handler, ok := b.Router(header.Name)
	if !ok {
		return nil
	}
	if err := handler(bee.NewCtx(ctx, &header, body)); err != nil {
		_ = b.sendRetryQueue(&header, body)
		return err
	}
	return nil
}

func (b *Broker) sendRetryQueue(header *codec.Header, body []byte) error {
	header.Retry++
	if header.Retry >= b.config.RetryMaxReconsume {
		return b.sendDeadLetterQueue(header, body)
	}
	data, err := b.config.Codec.Encode(header, body)
	if err != nil {
		return err
	}
	score := float64(time.Now().Add(b.config.RetryBackoff(header.Retry - 1)).UnixNano())
	err = b.c.ZAdd(context.Background(), b.config.Topic+":DELAY", &redis.Z{Score: score, Member: data}).Err()
	return err
}

func (b *Broker) sendDeadLetterQueue(header *codec.Header, body []byte) error {
	data, err := b.config.Codec.Encode(header, body)
	if err != nil {
		return err
	}
	err = b.c.RPush(context.Background(), b.config.Topic+":DeadLetter", data).Err()
	return err
}

func (b *Broker) getOneByDelayQueue(ctx context.Context) (err error) {
	var items []string
	var result []byte
	key := b.config.Topic + ":DELAY"
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
	if len(result) > 0 {
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
