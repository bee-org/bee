package amqp

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type Consumer struct {
	s         *Session
	buffer    chan amqp.Delivery
	ready     chan struct{}
	readyOnce sync.Once
}

func NewConsumer(s *Session) *Consumer {
	c := &Consumer{s: s, buffer: make(chan amqp.Delivery, s.config.Concurrency), ready: make(chan struct{})}
	go c.watch()
	<-c.ready
	return c
}

func (c *Consumer) watch() {
	s := c.s
	for {
		delivery, err := s.Stream()
		if err != nil {
			s.logger.Println("Stream failed. Retrying...")
			select {
			case <-s.done:
				return
			case <-time.After(defaultResendDelay):
			}
			continue
		}
		c.readyOnce.Do(func() { close(c.ready) })
		if c.delivery(delivery) {
			break
		}
	}
}

func (c *Consumer) delivery(delivery <-chan amqp.Delivery) bool {
	for {
		select {
		case <-c.s.done:
			close(c.buffer)
			return true
		case <-c.s.notifyConnClose:
			c.s.logger.Println("Connection closed. Reconnecting...")
			return false
		case <-c.s.notifyChanClose:
			c.s.logger.Println("Channel closed. Re-running init...")
			return false
		case msg, open := <-delivery:
			if !open {
				break
			}
			c.buffer <- msg
		}
	}
}

func (c *Consumer) GetBuffer() chan amqp.Delivery {
	return c.buffer
}
