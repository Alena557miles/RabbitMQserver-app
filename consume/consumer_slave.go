package consume

import (
	"context"
	"errors"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

var (
	errNotConnected = errors.New("Consumer: not connected to the server")
)

const (
	// When reconnecting to the server after connection failure
	ReconnectDelay = 5 * time.Second
	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second
)

type Slave struct {
	master          *Master
	channel         *amqp.Channel
	done            chan struct{}
	closed          chan struct{}
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	delivery        <-chan amqp.Delivery
	isReady         bool
	IsDeliveryReady bool
	chMux           *sync.Mutex
	prefetchCount   int
}

// NewSession creates a new consumer state instance, and automatically
// attempts to connect to the server.
func NewSlave(ctx context.Context, master *Master, prefetchCount int) *Slave {
	session := Slave{
		master:        master,
		done:          make(chan struct{}),
		chMux:         &sync.Mutex{},
		prefetchCount: prefetchCount,
	}
	session.init(ctx)

	return &session
}
func (s *Slave) InitStream(_ context.Context) (err error) {
	if !s.isReady {
		return errNotConnected
	}

	log.Printf("consume queue %s/%s\n", s.master.connection.Config.Vhost, s.master.qName)
	s.chMux.Lock()
	s.delivery, err = s.channel.Consume(
		s.master.qName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	s.chMux.Unlock()

	if err == nil {
		log.Println("Consumer: Stream SETUP")
		s.IsDeliveryReady = true
	}

	return
}

func (s *Slave) init(ctx context.Context) error {
	for {
		if s.master.connection == nil || s.master.connection.IsClosed() {
			log.Println("Consumer: connection not ready. Waiting...")
			time.Sleep(ReconnectDelay)
		} else {
			break
		}
	}

	ch, err := s.master.connection.Channel()
	if err != nil {
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		return err
	}

	err = ch.Qos(s.prefetchCount, 0, false)
	if err != nil {
		return err
	}

	s.changeChannel(ch)
	s.isReady = true
	s.closed = make(chan struct{})
	log.Println("Consumer: SETUP")
	return nil
}

func (s *Slave) changeChannel(channel *amqp.Channel) {
	s.channel = channel
	s.notifyChanClose = make(chan *amqp.Error, 1)
	s.notifyConfirm = make(chan amqp.Confirmation, 1)
	s.channel.NotifyClose(s.notifyChanClose)
	s.channel.NotifyPublish(s.notifyConfirm)
}
func (s *Slave) Closed() <-chan struct{} {
	return s.closed
}

func (s *Slave) GetStream() <-chan amqp.Delivery {
	s.chMux.Lock()
	d := s.delivery
	s.chMux.Unlock()

	return d
}
