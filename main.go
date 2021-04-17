package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"strconv"
	"time"
)

func main() {
	fmt.Printf("Running")

	run()
}

func run() {
	produce()
	consume()
}

func produce() {
	// to produce messages
	topic := "mytopic"
	partition := 0
	var err error

	conn, err := dialLeader(topic, partition)
	//conn := connectAndCreateTopic(topic, partition)
	//conn, err := dialNonLeader(topic, partition)
	if err != nil {
	    panic(err)
	}

	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func dialNonLeader(topic string, partition int) (*kafka.Conn, error) {
	// to connect to the kafka leader via an existing non-leader connection rather than using DialLeader
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return nil, err
	}
	var connLeader *kafka.Conn
	connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return nil, err
	}

	return connLeader, nil
}

func dialLeader(topic string, partition int) (*kafka.Conn, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	return conn, err
}

func connectAndCreateTopic(topic string, partition int) *kafka.Conn {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err.Error())
	}

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}

	return conn
}

func consume() {

}
