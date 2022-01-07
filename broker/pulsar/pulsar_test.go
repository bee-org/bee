package pulsar

import (
	"context"
	"github.com/fanjindong/bee/broker"
	"github.com/fanjindong/bee/example"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	ctx = context.Background()
	b   broker.IBroker
	err error
)

func TestMain(m *testing.M) {
	b, err = NewBroker(Config{
		URL:              os.Getenv("PULSAR_URL"),
		Topic:            "persistent://public/default/bee",
		SubscriptionName: "sub-test",
		AuthToken:        "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.bFEKUj29bpCRNPE5oKG5VxOFslkm5JKK4B-9zQrrpVIEy4_qvzu1AsyYB266jtuTPTzQVj8Kp8pffpTEg8GJuMQgt1VwDcgJ5k80pB5h6yjwnFqtP3TbLxvxdtIDAkQqyjbpstBD_57owJKS7l04YzpaiEJMTeNQqk7WIfYld5y7BFsSR7-8X6U6PoNk9E6S5R7XjLHvwquTFyUWdIHS-OeychhAKZPf415do5UliP42ZDQ1xfRxEJmwJD1vnzaZOQdP5y5zWFtkbhOACrI_ah2Tr9tMZcZvLgwt6T8ixaYhxG4hChjQQP97vZW65Wz1lcsnjl6Rvx5qwwu62iI89g",
		RetryEnable:      true,
		DLQ: &DLQPolicy{
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
	//b.Middleware(testFmtCostMw())
	if err = b.Worker(); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestPulSarBroker_SendPrint(t *testing.T) {
	type args struct {
		ctx  context.Context
		name string
		data interface{}
	}
	tests := []struct {
		name       string
		args       args
		wantErr    bool
		wantResult []string
	}{
		{args: args{ctx: ctx, name: "print", data: "a"}, wantResult: []string{"a"}},
		{args: args{ctx: ctx, name: "print", data: "b"}, wantResult: []string{"a", "b"}},
		{args: args{ctx: ctx, name: "print", data: "c"}, wantResult: []string{"a", "b", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := b.Send(tt.args.ctx, tt.args.name, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Send() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(1 * time.Second)
			want := make(map[string]struct{})
			for _, v := range tt.wantResult {
				want[v] = struct{}{}
			}
		})
	}
}

func TestPulSarBroker_SendCounter(t *testing.T) {
	type args struct {
		ctx   context.Context
		batch int
	}
	tests := []struct {
		name       string
		args       args
		wantErr    bool
		wantResult int64
	}{
		{args: args{ctx: ctx, batch: 1}, wantResult: 1},
		{args: args{ctx: ctx, batch: 10}, wantResult: 11},
		{args: args{ctx: ctx, batch: 100}, wantResult: 111},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wg := sync.WaitGroup{}
			for i := 0; i < tt.args.batch; i++ {
				wg.Add(1)
				go func() {
					if err := b.Send(tt.args.ctx, "counter", nil); (err != nil) != tt.wantErr {
						t.Errorf("SendCounter() error = %v, wantErr %v", err, tt.wantErr)
					}
					wg.Done()
				}()
			}
			wg.Wait()
			time.Sleep(time.Duration(tt.args.batch) * 100 * time.Millisecond)
			if example.CounterResult != tt.wantResult {
				t.Errorf("SendCounter() result = %v, want %v", example.CounterResult, tt.wantResult)
			}
		})
	}
}

func TestPulSarBroker_SendRetry(t *testing.T) {
	type args struct {
		ctx  context.Context
		data string
	}
	tests := []struct {
		name       string
		args       args
		wantErr    bool
		wantResult int
	}{
		{args: args{ctx: ctx, data: "err"}, wantResult: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := b.Send(tt.args.ctx, "error", tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Send() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(10 * time.Second)
			if example.ErrorCounter[tt.args.data] != tt.wantResult {
				t.Errorf("SendRetry() got = %v, want %v", example.ErrorCounter[tt.args.data], tt.wantResult)
			}
		})
	}
}

func TestPulSarBroker_SendDelay(t *testing.T) {
	type args struct {
		ctx   context.Context
		data  interface{}
		delay time.Duration
	}
	tests := []struct {
		name       string
		args       args
		wantErr    bool
		wantResult []string
	}{
		{args: args{ctx: ctx, delay: 0 * time.Second}},
		{args: args{ctx: ctx, delay: 3 * time.Second}},
		{args: args{ctx: ctx, delay: 5 * time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := time.Now().Add(tt.args.delay)
			if err := b.SendDelay(tt.args.ctx, "delay", tt.args.data, tt.args.delay); (err != nil) != tt.wantErr {
				t.Errorf("SendDelay() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := <-example.DelayResult
			if got.Before(want) || got.Sub(want).Seconds() > 1 {
				t.Errorf("SendDelay() got delay = %v, want %v", got.Second(), want.Second())
			}
		})
	}
}
