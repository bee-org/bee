package amqp

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"time"
)

// Session This exports a Session object that wraps this library. It
// automatically reconnects when the connection fails, and
// blocks all pushes until the connection succeeds. It also
// confirms every outgoing message, so none are lost.
// It doesn't automatically ack each message, but leaves that
// to the parent process, since it is usage-dependent.
//
// Try running this in one terminal, and `rabbitmq-server` in another.
// Stop & restart RabbitMQ to see how the queue reacts.
type Session struct {
	config          *Config
	logger          *log.Logger
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
	ready           chan struct{}
}

const (
	// When reconnecting to the server after connection failure
	defaultReconnectDelay = 5 * time.Second
	// When setting up the channel after a channel exception
	defaultReInitDelay = 2 * time.Second
	// When resending messages the server didn't confirm
	defaultResendDelay = 5 * time.Second
)

var (
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("session is shutting down")
)

// NewSession creates a new consumer state instance, and automatically
// attempts to connect to the server.
func NewSession(config *Config) *Session {
	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = defaultReconnectDelay
	}
	if config.ReInitDelay == 0 {
		config.ReInitDelay = defaultReInitDelay
	}
	if config.ResendDelay == 0 {
		config.ResendDelay = defaultResendDelay
	}
	session := Session{
		config: config,
		logger: log.New(os.Stdout, "", log.LstdFlags),
		done:   make(chan bool),
		ready:  make(chan struct{}),
	}
	go session.handleReconnect()
	<-session.ready
	return &session
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (s *Session) handleReconnect() {
	for {
		s.isReady = false
		s.logger.Println("Attempting to connect")

		conn, err := s.connect()

		if err != nil {
			s.logger.Println("Failed to connect. Retrying...")

			select {
			case <-s.done:
				return
			case <-time.After(s.config.ReconnectDelay):
			}
			continue
		}

		if done := s.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (s *Session) connect() (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error
	if s.config.TLSClientConfig != nil {
		conn, err = amqp.DialTLS(s.config.URL, s.config.TLSClientConfig)
	} else {
		conn, err = amqp.Dial(s.config.URL)
	}
	if err != nil {
		return nil, err
	}

	s.changeConnection(conn)
	s.logger.Println("Connected!")
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (s *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		s.isReady = false

		err := s.init(conn)

		if err != nil {
			s.logger.Println("Failed to initialize channel. Retrying...")

			select {
			case <-s.done:
				return true
			case <-time.After(s.config.ReInitDelay):
			}
			continue
		}

		select {
		case <-s.done:
			return true
		case <-s.notifyConnClose:
			s.logger.Println("Connection closed. Reconnecting...")
			return false
		case <-s.notifyChanClose:
			s.logger.Println("Channel closed. Re-running init...")
		}
	}
}

// init will initialize channel & declare queue
func (s *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	err = ch.Confirm(false)
	if err != nil {
		return err
	}

	if s.config.DelayedMessagePlugin {
		if err = ch.ExchangeDeclare(s.config.Exchange, "x-delayed-message", true, false, false, false, amqp.Table{"x-delayed-type": s.config.ExchangeType}); err != nil {
			return err
		}
	} else {
		if err = ch.ExchangeDeclare(s.config.Exchange, s.config.ExchangeType, true, false, false, false, nil); err != nil {
			return err
		}
	}
	if _, err = ch.QueueDeclare(s.config.Queue, true, false, false, false, nil); err != nil {
		return err
	}
	if err = ch.QueueBind(s.config.Queue, s.config.RoutingKey, s.config.Exchange, false, nil); err != nil {
		return err
	}

	s.changeChannel(ch)
	s.isReady = true
	close(s.ready)
	s.logger.Println("Setup!")
	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (s *Session) changeConnection(connection *amqp.Connection) {
	s.connection = connection
	s.notifyConnClose = make(chan *amqp.Error)
	s.connection.NotifyClose(s.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (s *Session) changeChannel(channel *amqp.Channel) {
	s.channel = channel
	s.notifyChanClose = make(chan *amqp.Error)
	s.notifyConfirm = make(chan amqp.Confirmation, 1)
	s.channel.NotifyClose(s.notifyChanClose)
	s.channel.NotifyPublish(s.notifyConfirm)
}

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (s *Session) Push(ctx context.Context, data []byte, delayMs int64) error {
	if !s.isReady {
		return errors.New("failed to push: not connected")
	}
	for {
		err := s.UnsafePush(ctx, data, delayMs)
		if err != nil {
			s.logger.Println("Push failed. Retrying...")
			select {
			case <-s.done:
				return errShutdown
			case <-time.After(s.config.ResendDelay):
			case <-ctx.Done():
				return err
			}
			continue
		}
		select {
		case confirm := <-s.notifyConfirm:
			if confirm.Ack {
				s.logger.Println("Push confirmed!")
				return nil
			}
		case <-time.After(s.config.ResendDelay):
		case <-ctx.Done():
			return err
		}
		s.logger.Println("Push didn't confirm. Retrying...")
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (s *Session) UnsafePush(ctx context.Context, data []byte, delayMs int64) error {
	if !s.isReady {
		return errNotConnected
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	var routingKey = s.config.RoutingKey
	if delayMs > 0 && !s.config.DelayedMessagePlugin {
		// It's necessary to redeclare the queue each time (to zero its TTL timer).
		queueName := fmt.Sprintf("%s.%d.DELAY", s.config.Queue, delayMs)
		declareQueueArgs := amqp.Table{
			// Exchange where to send messages after TTL expiration.
			"x-dead-letter-exchange": s.config.Exchange,
			// Routing key which use when resending expired messages.
			"x-dead-letter-routing-key": s.config.RoutingKey,
			// Time in milliseconds
			// after that message will expire and be sent to destination.
			"x-message-ttl": delayMs,
			// Time after that the queue will be deleted.
			"x-expires": delayMs * 2,
		}
		if _, err := s.channel.QueueDeclare(queueName, true, true, false, false, declareQueueArgs); err != nil {
			return err
		}
		routingKey = queueName
		if err := s.channel.QueueBind(queueName, routingKey, s.config.Exchange, false, nil); err != nil {
			return err
		}
	}
	msg := amqp.Publishing{Body: data}
	if s.config.DelayedMessagePlugin {
		msg.Headers = amqp.Table{"x-delay": delayMs}
	}
	return s.channel.Publish(s.config.Exchange, routingKey, false, false, msg)
}

// Stream will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (s *Session) Stream() (<-chan amqp.Delivery, error) {
	if !s.isReady {
		return nil, errNotConnected
	}
	return s.channel.Consume(s.config.Queue, "", false, false, false, false, nil)
}

// Close will cleanly shutdown the channel and connection.
func (s *Session) Close() error {
	if !s.isReady {
		return errAlreadyClosed
	}
	close(s.done)
	err := s.channel.Close()
	if err != nil {
		return err
	}
	err = s.connection.Close()
	if err != nil {
		return err
	}

	s.isReady = false
	return nil
}
