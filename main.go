package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Users struct {
	Id       int
	Name     string
	Email    string
	Phone    string
	Password string
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"addUser", // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			ConnMng(d.Body)
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func ConnMng(data_user []byte) {
	ctx := context.TODO()
	opts := options.Client().ApplyURI("mongodb://localhost:27017")

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		fmt.Printf("%s\n", err)
		panic(err)
	}

	defer client.Disconnect(ctx)

	fmt.Printf("%T\n", client)

	testDB := client.Database("testz")
	fmt.Printf("%T\n", testDB)

	exampleCollection := testDB.Collection("testz_example")

	fmt.Printf("%T\n", exampleCollection)

	var users_add Users

	err = json.Unmarshal(data_user, &users_add)
	if err != nil {
		fmt.Println(err)
	}
	example := bson.D{
		{"newuser", users_add},
	}
	r, err := exampleCollection.InsertOne(ctx, example)
	if err != nil {
		panic(err)
	}
	fmt.Println(r.InsertedID)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
