package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/fanjindong/bee/broker"
	"github.com/fanjindong/bee/example"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	var op string
	var name string
	var data string
	flag.StringVar(&op, "op", "consumer", "operation: producer,consumer")
	flag.StringVar(&name, "name", "", "name: handler")
	flag.StringVar(&data, "data", "", "data: any")
	flag.Parse()

	var b broker.IBroker
	var err error
	instanceName := strconv.Itoa(time.Now().Nanosecond())
	config := broker.RocketMQConfig{
		Hosts:             []string{"http://rmq1te.test.srv.mc.dd:9876", "http://rmq2te.test.srv.mc.dd:9876"},
		Topic:             "BEE",
		ProducerGroupName: "BEE-producer",
		ConsumerGroupName: "BEE-consumer",
		InstanceName:      instanceName,
		//AllocateStrategy:  consumer.AllocateByAveragely,
	}
	if op == "producer" {
		config.ConsumerGroupName = ""
	} else {
		config.ProducerGroupName = ""
	}
	b, err = broker.NewRocketMQBroker(config)
	if err != nil {
		panic(err)
	}
	b.Register("Print", example.PrintHandler)
	b.Register("Sleep", example.SleepHandler)
	if err = b.Worker(); err != nil {
		panic(err)
	}
	if op == "producer" {
		wg := sync.WaitGroup{}
		for _, item := range strings.Split(data, ",") {
			wg.Add(1)
			v := item
			go func() {
				err = b.Send(context.TODO(), name, v)
				fmt.Printf("producer %s,%s err:%v", name, v, err)
				wg.Done()
			}()
		}
		wg.Wait()
	} else {
		fmt.Println("consumer ready")
		block := make(chan struct{})
		<-block
	}
}
