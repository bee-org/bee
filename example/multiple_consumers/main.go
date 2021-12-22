package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/fanjindong/bee"
	"github.com/fanjindong/bee/broker"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func printHandler(c *bee.Context) error {
	var result string
	err := c.Parse(&result)
	fmt.Println("printHandler", result, err)
	return nil
}

func sleepHandler(c *bee.Context) error {
	var s string
	err := c.Parse(&s)
	d, _ := strconv.Atoi(s)
	time.Sleep(time.Duration(d) * time.Second)
	fmt.Println("sleepHandler", d, err)
	return err
}

func main() {
	var op string
	var name string
	var data string
	flag.StringVar(&op, "op", "consumer", "operation: producer,consumer")
	flag.StringVar(&name, "name", "", "name: handler")
	flag.StringVar(&data, "data", "", "data: any")
	flag.Parse()

	instanceName := GetLocalIP() + "@" + strconv.Itoa(os.Getpid())
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
	b, err := broker.NewRocketMQBroker(config)
	if err != nil {
		panic(err)
	}
	b.Register("print", printHandler)
	b.Register("sleep", sleepHandler)
	if err = b.Start(); err != nil {
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
		fmt.Println("consumer instance:", instanceName)
		block := make(chan struct{})
		<-block
	}
}

// GetLocalIP returns the non loopback local IP of the host
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
