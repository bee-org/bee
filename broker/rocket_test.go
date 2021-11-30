package broker

import (
	"context"
	"fmt"
	"github.com/fanjindong/bee"
	"os"
	"testing"
	"time"
)

var broker IBroker

func TestMain(m *testing.M) {
	initRocketMQBroker()
	defer broker.Close()
	os.Exit(m.Run())
}

func initRocketMQBroker() {
	b, err := NewRocketMQBroker(RocketMQConfig{
		Hosts:             []string{"http://rmq1te.test.srv.mc.dd:9876", "http://rmq2te.test.srv.mc.dd:9876"},
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
	broker = b
	return
}

//func TestRocketMQBroker_Send(t *testing.T) {
//	type args struct {
//		ctx  context.Context
//		name string
//		body interface{}
//	}
//	tests := []struct {
//		name    string
//		args    args
//		wantErr bool
//	}{
//		{args: args{ctx: context.TODO(), name: "print", body: 1}},
//	}
//	b := initRocketMQBroker()
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if err := b.Send(tt.args.ctx, tt.args.name, tt.args.body); (err != nil) != tt.wantErr {
//				t.Errorf("Send() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}

func printHandler(c *bee.Context) error {
	var result int64
	err := c.Parse(&result)
	fmt.Println("printHandler", result, err)
	return nil
}

func TestRocketMQBroker_Send(t *testing.T) {
	time.Sleep(1 * time.Second)
	broker.Send(context.TODO(), "print", time.Now().Second())
	//time.Sleep(1 * time.Second)
	//broker.Send(context.TODO(), "print", 5)
	//time.Sleep(1 * time.Second)
	//broker.Send(context.TODO(), "print", 6)
	time.Sleep(1 * time.Second)
}

func TestRocketMQBroker_Close(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := broker.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		time.Sleep(1 * time.Second)
	}
}
