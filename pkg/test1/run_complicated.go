package test1

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"strconv"
	"time"
)

func RunComplicated() {
	dialProduce()
	consume()
}

func dialProduce() {
	// to dialProduce messages
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
	//address := "b-1.yktest.5u4cj8.c6.kafka.eu-west-1.amazonaws.com:9092"
	//address := "b-1.yk-msk-2.tjvstm.c6.kafka.eu-west-1.amazonaws.com:9092"
	//address := "b-1.yk-msk-2.tjvstm.c6.kafka.eu-west-1.amazonaws.com:9092"
	address := "localhost:9092"
	conn, err := kafka.DialLeader(context.Background(), "tcp", address, topic, partition)
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
