# bee

![image](./images/favpng_honey.png)

An asynchronous message execution framework based on the producer consumer model.

# Features

supported middleware

- [x] redis
- [x] pulsar
- [x] rocketmq
- [ ] rabbitmq

# Quick start

```go
package main

import (
	"context"
	"fmt"
	"github.com/bee-org/bee"
	"github.com/bee-org/bee/broker/redis"
	"os"
)

func printHandler(c *bee.Context) error {
	var result int64
	err := c.Parse(&result)
	fmt.Println("printHandler", result, err)
	return nil
}

func main() {
	b, err := redis.NewBroker(redis.Config{
		URL:   os.Getenv("REDIS_URL"),
		Topic: "bee",
	})
	if err != nil {
		panic(err)
	}
	b.Register("print", printHandler)
	if err = b.Worker(); err != nil {
		panic(err)
	}

	b.Send(context.TODO(), "print", 1) // producer
	// output: printHandler 1 <nil>
}

```