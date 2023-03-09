package consume

import (
	"context"
	"github.com/streadway/amqp"
	"log"
)

type Master struct {
	qName           string
	connection      *amqp.Connection
	done            chan struct{}
	notifyConnClose chan *amqp.Error
	isReady         bool
}

func NewMaster(ctx context.Context, qName, busHost, busUser, busPass string) *Master {
	session := Master{
		qName: qName,
		done:  make(chan struct{}),
	}
	session.connect(ctx, busHost, busUser, busPass)

	return &session
}

func (s *Master) connect(ctx context.Context, busHost, busUser, busPass string) error {
	conn, err := amqp.DialConfig(busHost, amqp.Config{
		SASL: []amqp.Authentication{&amqp.PlainAuth{busUser, busPass}},
	})

	if err != nil {
		return err
	}

	s.changeConnection(conn)
	s.isReady = true
	log.Println("Consumer: CONNECTED")

	return nil
}

func (s *Master) changeConnection(connection *amqp.Connection) {
	s.connection = connection
	s.notifyConnClose = make(chan *amqp.Error, 1)
	s.connection.NotifyClose(s.notifyConnClose)
}
