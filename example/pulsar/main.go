package main

import (
	"context"
	"github.com/bee-org/bee/broker/pulsar"
	"github.com/bee-org/bee/example"
	"os"
	"time"
)

func main() {
	b, err := pulsar.NewBroker(pulsar.Config{
		URL:              os.Getenv("PULSAR_URL"),
		Topic:            "persistent://public/default/bee",
		SubscriptionName: "sub-test",
		AuthToken:        "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.bFEKUj29bpCRNPE5oKG5VxOFslkm5JKK4B-9zQrrpVIEy4_qvzu1AsyYB266jtuTPTzQVj8Kp8pffpTEg8GJuMQgt1VwDcgJ5k80pB5h6yjwnFqtP3TbLxvxdtIDAkQqyjbpstBD_57owJKS7l04YzpaiEJMTeNQqk7WIfYld5y7BFsSR7-8X6U6PoNk9E6S5R7XjLHvwquTFyUWdIHS-OeychhAKZPf415do5UliP42ZDQ1xfRxEJmwJD1vnzaZOQdP5y5zWFtkbhOACrI_ah2Tr9tMZcZvLgwt6T8ixaYhxG4hChjQQP97vZW65Wz1lcsnjl6Rvx5qwwu62iI89g",
		RetryEnable:      true,
		DLQ: &pulsar.DLQPolicy{
			MaxDeliveries:    3,
			DeadLetterTopic:  "sub-test-RETRY",
			RetryLetterTopic: "sub-test-DLQ",
		},
		NackRedeliveryDelay: 1 * time.Second,
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
