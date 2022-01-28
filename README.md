# bee

![image](./images/favpng_honey.png)

An asynchronous task execution framework based on the producer consumer model.

# Features

supported middleware

- [x] redis
- [x] pulsar
- [x] rocketmq
- [x] rabbitmq

# Quick start

Redis example:

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

RabbitMQ example:

```go
package main

import (
	"context"
	"fmt"
	"github.com/bee-org/bee"
	"github.com/bee-org/bee/broker/amqp"
	"os"
)

func printHandler(c *bee.Context) error {
	var result int64
	err := c.Parse(&result)
	fmt.Println("printHandler", result, err)
	return nil
}

func main() {
	b, err := amqp.NewBroker(amqp.Config{
		URL:   os.Getenv("RABBIT_URL"),
		Queue: "bee",
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

# Message Protocol

```text
bit:0-------8------16------24----------------n
    ┌───────┬───────┬───────┬────────────────┐
    │version│ retry │length │     name       │ header
    ├───────┴───────┴───────┴────────────────┤
    │                  body                  │
    └────────────────────────────────────────┘
```

- version(8bit): Protocol Version number
- retry(8bit): The number of times a message was redelivered
- length(8bit): The length of the function name that follows
- name(n-bit): The function name
- body(n-bit): Data required for function execution