package main

import (
	"fmt"
	"github.com/bee-org/bee"
	"github.com/bee-org/bee/broker/redis"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func PrintHandler(c *bee.Context) error {
	var result string
	err := c.Parse(&result)
	fmt.Println(time.Now(), "printHandler", result, err)
	return nil
}

func main() {
	b, err := redis.NewBroker(redis.Config{
		URL:               os.Getenv("REDIS_URL"),
		Topic:             "bee",
		RetryMaxReconsume: 3,
	})
	if err != nil {
		panic(err)
	}
	b.Register("print", PrintHandler)
	if err = b.Worker(); err != nil {
		panic(err)
	}
	defer b.Close()

	exit := make(chan os.Signal)
	signal.Notify(exit, syscall.SIGQUIT)
	<-exit
}
