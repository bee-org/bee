# bee

![image](./images/bee_PNG74650.png)

An asynchronous message execution framework based on the producer consumer model.

# Features

supported middleware

-[ ] redis
-[x] pulsar
-[x] rocketmq
-[ ] rabbitmq

# Quick start

```go
import (
    "github.com/fanjindong/bee"
)

func printHandler(c *bee.Context) error {
    var result int64
    err := c.Parse(&result)
    fmt.Println("printHandler", result, err)
    return nil
}

func main(){
    b, err := bee.NewRocketMQBroker(RocketMQConfig{
        Hosts:             []string{"http://rmq.test:9876"},
        Topic:             "BEE",
        ProducerGroupName: "BEE-producer",
        ConsumerGroupName: "BEE-consumer",
        Order:             false,
        BroadCasting:      false,
    })
    if err != nil {
        panic(err)
    }
    b.Register("print", printHandler)
    if err = b.Start(); err != nil {
        panic(err)
    }
    
	// producer
    b.Send(context.TODO(), "print", 1)
	// consumer output: printHandler 7 <nil>
}

```