package amqp

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type Consumer struct {
	s      *Session
	buffer chan amqp.Delivery
	ready  chan struct{}
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
		close(c.ready)
		for {
			select {
			case <-s.done:
				close(c.buffer)
				return
			case <-s.notifyConnClose:
				s.logger.Println("Connection closed. Reconnecting...")
				break
			case <-s.notifyChanClose:
				s.logger.Println("Channel closed. Re-running init...")
				break
			case msg := <-delivery:
				c.buffer <- msg
			}
		}
	}
}

func (c *Consumer) GetBuffer() chan amqp.Delivery {
	return c.buffer
}
