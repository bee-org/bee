package example

import (
	"fmt"
	"github.com/bee-org/bee"
	"sync/atomic"
	"time"
)

var PrintResult = map[string]struct{}{}

func PrintHandler(c *bee.Context) error {
	var result string
	err := c.Parse(&result)
	fmt.Println("printHandler", c.Message().GetMsgId(), result, err)
	PrintResult[result] = struct{}{}
	return nil
}

func SleepHandler(c *bee.Context) error {
	var d time.Duration
	err := c.Parse(&d)
	select {
	case <-c.Done():
		fmt.Println("SleepHandler", d.String(), c.Err())
		return c.Err()
	case <-time.After(d):
	}
	fmt.Println("SleepHandler", d.String(), err)
	return err
}

var CounterResult int64

func CounterHandler(c *bee.Context) error {
	atomic.AddInt64(&CounterResult, 1)
	fmt.Println("counterHandler:", CounterResult)
	return nil
}

var ErrorCounter = make(map[string]int)

func ErrorHandler(c *bee.Context) error {
	var s string
	c.Parse(&s)
	err := fmt.Errorf(s)
	fmt.Println("errorHandler:", err)
	ErrorCounter[s] += 1
	return err
}

var DelayResult = make(chan time.Time, 1)

func DelayHandler(c *bee.Context) error {
	DelayResult <- time.Now()
	fmt.Println("delayHandler:", time.Now().String())
	return nil
}

func PanicHandler(c *bee.Context) error {
	panic("panic")
}
