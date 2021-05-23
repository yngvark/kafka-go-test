package test4

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"math/rand"
	"time"
)

func Run() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    makeTopic(),
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  100 * time.Millisecond,
	})

	defer func() {
		err := reader.Close()
		if err != nil {
			fmt.Printf("Error when closing: %s", err)
		}
	}()

	err := writeAndConsumeMessages(ctx, reader)
	if err != nil {
		fmt.Printf("reading messages: %s", err)
	}

	fmt.Println("Done")
}

func makeTopic() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprintf("yktest-kafka-go-%016x", r.Int63())
}

func writeAndConsumeMessages(ctx context.Context, r *kafka.Reader) error {
	const N = 3
	err := prepareReader(ctx, r, createMessages(N)...)
	if err != nil {
		return fmt.Errorf("preparing reader: %w", err)
	}

	fmt.Println("Reading messages")

	for i := 0; i < 10; i++ {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("reading message. Failed: %w", err)
		}

		fmt.Printf("message: %s\n", string(m.Value))

		if string(m.Value) == "/quit" {
			fmt.Println("Got /quit msg, quitting")
			break
		}
	}

	return nil
}

func prepareReader(ctx context.Context, r *kafka.Reader, msgs ...kafka.Message) error {
	var config = r.Config()
	var conn *kafka.Conn
	var err error

	for {
		if conn, err = kafka.DialLeader(ctx, "tcp", "localhost:9092", config.Topic, config.Partition); err == nil {
			break
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	defer conn.Close()

	fmt.Println("Writing messages")
	if _, err := conn.WriteMessages(msgs...); err != nil {
		return fmt.Errorf("writing messages: %w", err)
	}

	return nil
}

func createMessages(n int) []kafka.Message {
	base := time.Now()
	msgs := make([]kafka.Message, n + 1)

	for i := 0; i != n; i++ {
		msgs[i] = kafka.Message{
			Time:  base.Add(time.Duration(i) * time.Millisecond).Truncate(time.Millisecond),
			Value: []byte("hello!!"),
		}
	}

	msgs[n-1] = kafka.Message{
		Value: []byte("/quit"),
	}
	return msgs
}