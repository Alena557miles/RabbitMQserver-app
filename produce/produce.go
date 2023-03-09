package produce

import (
	"context"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Broker struct {
	exName          string
	connection      *amqp.Connection
	done            chan struct{}
	notifyConnClose chan *amqp.Error
	isReady         bool
	ackTag          chan uint64
}

func NewBroker(ctx context.Context, exName, busHost, busUser, busPass string) *Broker {
	session := Broker{
		exName: exName,
		done:   make(chan struct{}),
	}
	session.connect(ctx, busHost, busUser, busPass)
	return &session
}
func (s *Broker) connect(_ context.Context, busHost, busUser, busPass string) error {
	conn, err := amqp.DialConfig(
		busHost,
		amqp.Config{SASL: []amqp.Authentication{&amqp.PlainAuth{busUser, busPass}}})
	if err != nil {
		return err
	}
	s.changeConnection(conn)
	s.isReady = true
	s.done = make(chan struct{})
	log.Println("Producer: CONNECTED")
	return nil
}

func (s *Broker) changeConnection(connection *amqp.Connection) {
	s.connection = connection
	s.notifyConnClose = make(chan *amqp.Error, 1)
	s.connection.NotifyClose(s.notifyConnClose)
	log.Println("Producer: Connection was closed")
}

func (s *Broker) Close() error {
	if !s.isReady {
		return errors.New(fmt.Sprintf("Producer: connection not ready while closing"))
	}
	err := s.connection.Close()
	if err != nil {
		return err
	}
	s.isReady = false

	return nil
}
