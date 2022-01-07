package example

import (
	"fmt"
	"github.com/fanjindong/bee"
	"sync/atomic"
	"time"
)

var PrintResult = map[string]struct{}{}

func PrintHandler(c *bee.Context) error {
	var result string
	err := c.Parse(&result)
	fmt.Println("printHandler", result, err)
	PrintResult[result] = struct{}{}
	return nil
}

func SleepHandler(c *bee.Context) error {
	var d time.Duration
	err := c.Parse(&d)
	time.Sleep(d)
	return err
}

var CounterResult int64

func CounterHandler(c *bee.Context) error {
	atomic.AddInt64(&CounterResult, 1)
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
	return nil
}
