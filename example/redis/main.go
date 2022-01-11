package main

import (
	"context"
	"github.com/bee-org/bee/broker/redis"
	"github.com/bee-org/bee/example"
	"os"
	"time"
)

func main() {
	b, err := redis.NewBroker(redis.Config{
		URL:               os.Getenv("REDIS_URL"),
		Topic:             "bee",
		RetryMaxReconsume: 3,
	})
	if err != nil {
		panic(err)
	}
	b.Register("print", example.PrintHandler)
	b.Register("sleep", example.SleepHandler)
	b.Register("counter", example.CounterHandler)
	b.Register("error", example.ErrorHandler)
	b.Register("delay", example.DelayHandler)
	if err = b.Worker(); err != nil {
		panic(err)
	}
	defer b.Close()

	b.Send(context.Background(), "print", "hello world!")
	b.Send(context.Background(), "sleep", 1*time.Second)
	b.Send(context.Background(), "counter", nil)
	b.Send(context.Background(), "error", "err example")
	b.SendDelay(context.Background(), "delay", nil, 3*time.Second)
	time.Sleep(5 * time.Second)
}
