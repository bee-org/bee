package broker

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

var ctx = context.Background()

func initPulSarBroker() IBroker {
	b, err := NewPulSarBroker(PulsarConfig{
		URL:                 os.Getenv("PULSAR_URL"),
		Topic:               "persistent://ddmc/test/bee",
		SubscriptionName:    "sub-test",
		AuthToken:           "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.w5mhJRHR1i-3pbHNqGqeVHO16KPB4GkqEIStoHey8cdQNDnLnAED4SdQh_QOi2aHom6hbJ4fUxR0M156CuvPcz3LtSwwubJgPva9AgMFLLFS8mTDCP_BX_F8_WxtKkL7j3aWx38qbO2-P_5o9Jpa5Il3xoakP7vcL-aO2KHrp7XpJwZCw-MSthSc6ZDhIWNOs0UnKqSBzkNF1rDmgy0t-x4ppvgsCNui_kymtVREKpu72imSSDpQhdbzhbBijCZFsL23LxLe7jt8Iox9B_qw71bKAyLrOGZjgcse9HYmUBeCKfIX9A_JJmNn1I-oCT3t1ULJaFq68ToMFczTmFNe1w",
		RetryEnable:         true,
		NackRedeliveryDelay: 1 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	b.Register("print", printHandler)
	b.Register("sleep", sleepHandler)
	b.Register("counter", counterHandler)
	b.Register("error", errorHandler)
	b.Register("delay", delayHandler)
	//b.Middleware(testFmtCostMw())
	if err = b.Worker(); err != nil {
		panic(err)
	}
	return b
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
	b := initPulSarBroker()
	defer func() { _ = b.Close() }()

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
	b := initPulSarBroker()
	defer func() { _ = b.Close() }()

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
			if counterResult != tt.wantResult {
				t.Errorf("SendCounter() result = %v, want %v", counterResult, tt.wantResult)
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
		wantResult int64
	}{
		{args: args{ctx: ctx, data: "3"}, wantResult: 1},
	}
	b := initPulSarBroker()
	defer func() { _ = b.Close() }()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := b.Send(tt.args.ctx, "error", tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Send() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(30 * time.Second)
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
	b := initPulSarBroker()
	defer func() { _ = b.Close() }()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := time.Now().Add(tt.args.delay)
			if err := b.SendDelay(tt.args.ctx, "delay", tt.args.data, tt.args.delay); (err != nil) != tt.wantErr {
				t.Errorf("SendDelay() error = %v, wantErr %v", err, tt.wantErr)
			}
			got := <-delayResult
			if got.Before(want) || got.Sub(want).Seconds() >= 1 {
				t.Errorf("SendDelay() got delay = %v, want %v", got.Second(), want.Second())
			}
		})
	}
}
