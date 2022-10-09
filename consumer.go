package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	conn, err := amqp.Dial("amqp://root:root@localhost:5672/")
	logError(err)
	defer conn.Close()

	ch, err := conn.Channel()
	logError(err)
	defer ch.Close()

	go consume(ch)
	time.Sleep(time.Hour * 1)
}

func consume(ch *amqp.Channel) {

	err := ch.ExchangeDeclare(
		"example.fanout",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	logError(err)

	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	logError(err)

	err = ch.QueueBind(
		q.Name,
		"",
		"example.fanout",
		false,
		nil,
	)
	logError(err)

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	logError(err)

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			fmt.Printf("Recieved Message: %s\n", d.Body)
		}
	}()

	log.Printf("Waiting for message.")
	<-forever
}

func logError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
