package main

import (
	"context"
	"fmt"
	"github.com/bee-org/bee/broker/redis"
	"os"
	"time"
)

var ctx = context.Background()

func main() {
	var (
		name  string
		delay int
		arg   string
	)
	b, err := redis.NewBroker(redis.Config{
		URL:   os.Getenv("REDIS_URL"),
		Topic: "bee",
	})
	if err != nil {
		panic(err)
	}
	for {
		fmt.Println("input: name arg delay(s)")
		_, _ = fmt.Scanln(&name, &arg, &delay)
		if delay > 0 {
			fmt.Println(time.Now(), "SendDelay", name, arg, time.Duration(delay)*time.Second)
			_ = b.SendDelay(ctx, name, arg, time.Duration(delay)*time.Second)
		} else {
			fmt.Println(time.Now(), "Send", name, arg, time.Duration(delay)*time.Second)
			_ = b.Send(ctx, name, arg)
		}
	}
}
