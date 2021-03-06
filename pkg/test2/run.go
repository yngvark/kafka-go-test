package test2

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"strconv"
	"time"
)

func Run() {
	produce(context.Background())
}

// the topic and broker address are initialized as constants
const (
	topic          = "message-log"
	//broker1Address = "localhost:9093"
	//broker2Address = "localhost:9092"
	//broker3Address = "localhost:9095"
	broker1Address = "b-1.yk-msk-2.tjvstm.c6.kafka.eu-west-1.amazonaws.com:9092"
	broker2Address = "b-2.yk-msk-2.tjvstm.c6.kafka.eu-west-1.amazonaws.com:9092"
)

func produce(ctx context.Context) {
	// initialize a counter
	i := 0

	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		//Brokers: []string{broker1Address, broker2Address, broker3Address},
		Brokers: []string{broker1Address, broker2Address},
		Topic:   topic,
	})

	for {
		// each kafka message has a key and value. The key is used
		// to decide which partition (and consequently, which broker)
		// the message gets published on
		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// create an arbitrary message payload for the value
			Value: []byte("this is message" + strconv.Itoa(i)),
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}

		// log a confirmation once the message is written
		fmt.Println("writes:", i)
		i++
		// sleep for a second
		time.Sleep(time.Second)
	}
}