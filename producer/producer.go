package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/radiophysiker/kafka-hw1/models"
)

// main initializes a Kafka producer, sends serialized messages to a topic in a loop, and handles termination signals.
func main() {
	// Config for Kafka producer
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094,localhost:9095",
		"acks":              "all",
		"retries":           3,
	}

	p, err := kafka.NewProducer(config)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		// Обработка событий от Kafka
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("[Producer] Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("[Producer] Delivered message to %v [%d] at offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()
	// Канал для обработки сигналов завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	topic := "my-topic"
	// Счетчик сообщений
	id := 0

	fmt.Println("[Producer] Starting producing messages...")
	for {
		select {
		case <-sigChan:
			// Обработка сигнала завершения
			fmt.Println("[Producer] Stopping producer...")
			return
		default:
			// Генерация сообщения
			message := &models.Message{
				ID:      id,
				Content: fmt.Sprintf("Message: #%d", id),
				SentAt:  time.Now(),
			}
			// Сериализация сообщения для отправки
			data, err := message.Serialize()
			if err != nil {
				fmt.Printf("[Producer] Serialization error: %v\n", err)
				continue
			}
			// Отправка сообщения
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          data,
			}, nil)
			if err != nil {
				fmt.Printf("[Producer] Failed to produce message: %v\n", err)
			} else {
				fmt.Printf("[Producer] Sent message ID=%d\n", id)
			}
			// Увеличение счетчика
			id++
			// Задержка перед отправкой следующего сообщения
			time.Sleep(5 * time.Second)
		}
	}
}
