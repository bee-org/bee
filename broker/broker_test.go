package broker

import (
	"fmt"
	"github.com/fanjindong/bee"
	"sync/atomic"
	"time"
)

var printResult = map[string]struct{}{}

func printHandler(c *bee.Context) error {
	var result string
	err := c.Parse(&result)
	fmt.Println("printHandler", result, err)
	printResult[result] = struct{}{}
	return nil
}

func sleepHandler(c *bee.Context) error {
	var d time.Duration
	err := c.Parse(&d)
	time.Sleep(d)
	return err
}

var counterResult int64

func counterHandler(c *bee.Context) error {
	atomic.AddInt64(&counterResult, 1)
	return nil
}

func errorHandler(c *bee.Context) error {
	var s string
	c.Parse(&s)
	err := fmt.Errorf(s)
	fmt.Println("errorHandler:", err)
	return err
}

var delayResult = make(chan time.Time, 1)

func delayHandler(c *bee.Context) error {
	delayResult <- time.Now()
	return nil
}
