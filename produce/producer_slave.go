package produce

import (
	"context"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

const (
	// When reconnecting to the server after connection failure
	ReconnectDelay = 5 * time.Second
	// When resending messages the server didn't confirm
	resendDelay    = 15 * time.Second
	confirmRetries = 9
	pushRetries    = 3
)

var errShutdown = errors.New("-- Producer session shut down")
var errConnNotReady = errors.New("Producer: connection not ready")

type Slave struct {
	broker          *Broker
	channel         *amqp.Channel
	done            chan struct{}
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	notifyFlow      chan bool
	IsReady         bool
	tm              *time.Ticker
}

func NewSlave(ctx context.Context, master *Broker) *Slave {
	session := Slave{
		broker: master,
		done:   make(chan struct{}),
		tm:     time.NewTicker(resendDelay),
	}
	session.init(ctx)
	return &session
}

func (s *Slave) init(ctx context.Context) error {
	for {
		if s.broker.connection == nil || s.broker.connection.IsClosed() {
			log.Println("Producer: connection not ready. Waiting... (from init Slave)")
			time.Sleep(ReconnectDelay)
		} else {
			break
		}
	}

	ch, err := s.broker.connection.Channel()
	if err != nil {
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		return err
	}

	err = s.declarationAndBinding(ctx, ch)
	if err != nil {
		return err
	}

	s.changeChannel(ctx, ch)
	s.IsReady = true
	s.done = make(chan struct{})
	log.Println("Producer: SETUP")
	return nil
}

func (s *Slave) declarationAndBinding(_ context.Context, ch *amqp.Channel) (err error) {
	qName := "first"
	key := "key-product"
	_, err = ch.QueueDeclare(
		qName,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		return
	}
	err = ch.QueueBind(
		qName,
		key,
		s.broker.exName,
		false,
		nil)
	if err != nil {
		return
	}
	return
}

func (s *Slave) changeChannel(ctx context.Context, channel *amqp.Channel) {
	s.channel = channel
	s.notifyChanClose = make(chan *amqp.Error, 1)
	s.notifyConfirm = make(chan amqp.Confirmation, 1)
	s.channel.NotifyClose(s.notifyChanClose)
	s.channel.NotifyPublish(s.notifyConfirm)
	s.notifyFlow = make(chan bool, 1)
	s.channel.NotifyFlow(s.notifyFlow)

	go s.listenFlow(ctx)
}
func (s *Slave) listenFlow(_ context.Context) {
	for {
		select {
		case res, ok := <-s.notifyFlow:
			log.Println("Producer: receive notifyFlow = %v, is closed = %v", res, ok)
			if !ok {
				return
			}
		}
	}
}

func (s *Slave) UnsafePush(rk string, body []byte) error {
	if !s.IsReady {
		return errors.New(fmt.Sprintf("Producer: connection not ready!!!"))
	}

	return s.channel.Publish(
		s.broker.exName,
		rk,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/octet-stream",
			Body:         body,
			Priority:     5,
		},
	)
}

func (s *Slave) Push(_ context.Context, rk string, body []byte) error {
	tm := time.NewTicker(resendDelay)
	defer tm.Stop()

	retries := 0
	for {
		if !s.IsReady {
			if retries > pushRetries {
				return errors.New("Producer: failed to push")
			} else {
				log.Println("Producer: failed to push. Retrying...")
				retries++
				time.Sleep(ReconnectDelay)
			}
		} else {
			break
		}
	}

	retries = 0
	for {
		if !s.IsReady {
			return errConnNotReady
		}

		err := s.UnsafePush(rk, body)

		if err != nil {
			log.Println("Producer: Push failed: %s. (%s) Retrying...", err, rk)
			select {
			case <-s.broker.done:
				log.Println("receive done signal from master %s", rk)
				return errShutdown
			case <-s.done:
				log.Println("receive done signal %s", rk)
				return errShutdown
			case <-tm.C:
			}
			continue
		}

		for {
			if !s.IsReady {
				return errConnNotReady
			}
			select {
			case confirm := <-s.notifyConfirm:
				if confirm.Ack {
					log.Println("Producer: published successfully into %s", rk)
					return nil
				} else {
					log.Println("producer_slave, NOT Acked to %s", rk)
				}
			case <-s.broker.done:
				log.Println("receive done signal from master to %s", rk)
				return nil
			case <-s.done:
				log.Println("receive done signal to %s", rk)
				return nil
			case <-tm.C:
				log.Println("producer_slave, relisten to %s", rk)
			}
			if s.broker.connection.IsClosed() {
				return errConnNotReady
			}
			if retries > confirmRetries {
				return fmt.Errorf("Producer: failed to confirm to %s", rk)
			} else {
				retries++
				log.Println("Producer: failed to confirm. Retrying... to %s", rk)
			}
		}
	}
}
