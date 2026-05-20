package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string
type AckType string

const (
	ACK         AckType = "ACK"
	NACKRequeue AckType = "NACKREQUEUE"
	NACKDiscard AckType = "NACKDISCARD"
)

const (
	SimpleQueueTypeDurable   SimpleQueueType = "durable"
	SimpleQueueTypeTransient SimpleQueueType = "transient"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	json, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        json,
	})
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        buf.Bytes(),
	})
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to open a channel: %w", err)
	}
	args := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}
	queue, err := ch.QueueDeclare(
		queueName,
		queueType == SimpleQueueTypeDurable,
		queueType == SimpleQueueTypeTransient,
		queueType == SimpleQueueTypeTransient,
		false,
		args,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	// Bind the queue to the exchange with the given key
	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for msg := range msgs {
			var t T
			err := json.Unmarshal(msg.Body, &t)
			if err != nil {
				continue
			}
			switch handler(t) {
			case ACK:
				msg.Ack(false)
				fmt.Println("msg successfully ack")
			case NACKDiscard:
				msg.Nack(false, false)
				fmt.Println("msg successfully nackdiscard")
			case NACKRequeue:
				msg.Nack(false, true)
				fmt.Println("msg successfully nackrequeue")

			}

		}
	}()
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	err = ch.Qos(20, 0, false)
	if err != nil {
		return err
	}
	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for msg := range msgs {
			var t T
			decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
			err := decoder.Decode(&t)
			if err != nil {
				msg.Nack(false, false)
				fmt.Println("[SubscribeGob] error decoding message:", err)
				continue
			}
			switch handler(t) {
			case ACK:
				msg.Ack(false)
				fmt.Println("[SubscribeGob] msg successfully ack")
			case NACKDiscard:
				msg.Nack(false, false)
				fmt.Println("[SubscribeGob] msg successfully nackdiscard")
			case NACKRequeue:
				msg.Nack(false, true)
				fmt.Println("[SubscribeGob] msg successfully nackrequeue")

			}

		}
	}()
	return nil
}
