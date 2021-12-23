package main

import (
	"context"
	"fmt"
	"github.com/fanjindong/bee/broker"
	"github.com/fanjindong/bee/example"
	"strings"
	"sync"
)

func main() {
	var b broker.IBroker
	var err error
	b, err = broker.NewPulSarBroker(broker.PulsarConfig{
		URL:              "pulsar://10.20.248.134:6650",
		Topic:            "persistent://ddmc/test/bee",
		SubscriptionName: "sub-test",
		AuthToken:        "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.w5mhJRHR1i-3pbHNqGqeVHO16KPB4GkqEIStoHey8cdQNDnLnAED4SdQh_QOi2aHom6hbJ4fUxR0M156CuvPcz3LtSwwubJgPva9AgMFLLFS8mTDCP_BX_F8_WxtKkL7j3aWx38qbO2-P_5o9Jpa5Il3xoakP7vcL-aO2KHrp7XpJwZCw-MSthSc6ZDhIWNOs0UnKqSBzkNF1rDmgy0t-x4ppvgsCNui_kymtVREKpu72imSSDpQhdbzhbBijCZFsL23LxLe7jt8Iox9B_qw71bKAyLrOGZjgcse9HYmUBeCKfIX9A_JJmNn1I-oCT3t1ULJaFq68ToMFczTmFNe1w",
	})
	if err != nil {
		panic(err)
	}
	b.Register("Print", example.PrintHandler)
	b.Register("Sleep", example.SleepHandler)
	if err = b.Worker(); err != nil {
		panic(err)
	}
	fmt.Println("consumer ready")
	fmt.Println("input send params: name, data")
	var name string
	var data string
	for {
		if _, err = fmt.Scan(&name, &data); err != nil {
			fmt.Println("input err:", err)
			continue
		}
		wg := sync.WaitGroup{}
		for _, item := range strings.Split(data, ",") {
			wg.Add(1)
			v := item
			go func() {
				err = b.Send(context.TODO(), name, v)
				fmt.Printf("producer %s,%s err:%v\n", name, v, err)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
