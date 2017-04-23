package cony

import "github.com/streadway/amqp"

// Declaration is a callback type to declare AMQP queue/exchange/binding
type Declaration func(Declarer) error

// Declarer is implemented by *amqp.Channel
type Declarer interface {
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
}

// DeclareQueue is a way to declare AMQP queue
func DeclareQueue(q *Queue) Declaration {
	name := q.Name
	return func(c Declarer) error {
		q.Name = name
		realQ, err := c.QueueDeclare(q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false,
			q.Args,
		)
		q.l.Lock()
		q.Name = realQ.Name
		q.l.Unlock()
		return err
	}
}

// DeclareExchange is a way to declare AMQP exchange
func DeclareExchange(e Exchange) Declaration {
	return func(c Declarer) error {
		return c.ExchangeDeclare(e.Name,
			e.Kind,
			e.Durable,
			e.AutoDelete,
			false,
			false,
			e.Args,
		)
	}
}

// DeclareQueueBinding is a way to declare AMQP binding between AMQP queue and exchange
func DeclareQueueBinding(b QueueBinding) Declaration {
	return func(c Declarer) error {
		var err error

		if b.Keys == nil {
			return c.QueueBind(b.Queue.Name, "#.#", b.Exchange.Name, false, b.Args)
		}

		for _, key := range b.Keys {
			err = c.QueueBind(b.Queue.Name, key, b.Exchange.Name, false, b.Args)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func DeclareExchangeBinding(b ExchangeBinding) Declaration {
	return func(c Declarer) error {
		var err error
		if b.Keys == nil {
			return c.ExchangeBind(b.DestinationExchange.Name, "#.#", b.SourceExchange.Name, false, b.Args)
		}
		for _, key := range b.Keys {
			err = c.ExchangeBind(b.DestinationExchange.Name, key, b.SourceExchange.Name, false, b.Args)
			if err != nil {
				return err
			}
		}
		return nil
	}
}
