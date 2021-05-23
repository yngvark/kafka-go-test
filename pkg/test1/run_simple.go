package test1

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"math/rand"
	"strconv"
	"time"
)

func RunSimple() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    makeTopic(),
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  100 * time.Millisecond,
	})
	defer func(r *kafka.Reader) {
		err := r.Close()
		if err != nil {
			fmt.Printf("Error when closing: %s", err)
		}
	}(r)

	err := testReaderReadMessages(ctx, r)
	if err != nil {
	    fmt.Printf("reading messages: %s", err)
	}

	fmt.Println("Done")
}

func makeTopic() string {
	return fmt.Sprintf("kafka-go-%016x", rand.Int63())
}

func testReaderReadMessages(ctx context.Context, r *kafka.Reader) error {
	const N = 5
	err := prepareReader(ctx, r, makeTestSequence(N)...)
	if err != nil {
	    return fmt.Errorf("preparing reader: %w", err)
	}

	var offset int64

	for i := 0; i != N; i++ {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("reading message at offset %d. Failed: %w", offset, err)
		}

		fmt.Printf("message: %s\n", string(m.Value))

		offset = m.Offset + 1
		v, _ := strconv.Atoi(string(m.Value))
		if v != i {
			return fmt.Errorf("message at index %d has wrong value: %d", i, v)
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

	if _, err := conn.WriteMessages(msgs...); err != nil {
		return fmt.Errorf("writing messages: %w", err)
	}

	return nil
}

func makeTestSequence(n int) []kafka.Message {
	base := time.Now()
	msgs := make([]kafka.Message, n)
	for i := 0; i != n; i++ {
		msgs[i] = kafka.Message{
			Time:  base.Add(time.Duration(i) * time.Millisecond).Truncate(time.Millisecond),
			//Value: []byte(strconv.Itoa(i)),
			//Value: []byte("hello" + strconv.Itoa(i)),
			Value: []byte("hello!!"),
		}
	}
	return msgs
}