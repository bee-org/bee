package example

import (
	"fmt"
	"github.com/fanjindong/bee"
	"strconv"
	"time"
)

func PrintHandler(c *bee.Context) error {
	var result string
	err := c.Parse(&result)
	fmt.Println("printHandler", result, err)
	return nil
}

func SleepHandler(c *bee.Context) error {
	var s string
	err := c.Parse(&s)
	d, _ := strconv.Atoi(s)
	time.Sleep(time.Duration(d) * time.Second)
	fmt.Println("sleepHandler", d, err)
	return err
}
