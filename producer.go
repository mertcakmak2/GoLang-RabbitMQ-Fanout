package main

import (
	"fmt"
	"log"
	"os"
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

	// produceAndConsume(ch)
	// or
	go func() {
		for range time.Tick(time.Second * 3) {
			produce(ch)
		}
	}()
	consume(ch)
}

func produceAndConsume(ch *amqp.Channel) {

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

	message := os.Args[1]

	err = ch.Publish(
		"example.fanout",
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)

	log.Printf(" [x] Sent %s", message)

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

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

func produce(ch *amqp.Channel) {

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

	message := os.Args[1]

	err = ch.Publish(
		"example.fanout",
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)

	log.Printf("Sent %s", message)

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
