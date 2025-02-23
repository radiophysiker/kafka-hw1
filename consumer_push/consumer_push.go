package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/radiophysiker/kafka-hw1/models"
)

func main() {
	// Config for Kafka consumer push
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094,localhost:9095",
		"group.id":          "push-consumer-group",
		// Автоматический коммит оффсетов
		"enable.auto.commit": true,
		"auto.offset.reset":  "earliest",
		"fetch.min.bytes":    1024,
	}

	c, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	topic := "my-topic"
	// Подписываемся на топик
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		panic(err)
	}

	fmt.Println("[Push Consumer] Starting...")

	for {
		ev := c.Poll(0) // Очень короткий таймаут
		if ev == nil {
			// Если нет сообщений, ждём 50 мс
			time.Sleep(50 * time.Millisecond)
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			// Получено новое сообщение
			msg, err := models.Deserialize(e.Value)
			if err != nil {
				fmt.Printf("[Push Consumer] Deserialization error: %v\n", err)
				continue
			}
			fmt.Printf("[Push Consumer] Received message: ID=%d, Content=%s\n", msg.ID, msg.Content)
		case kafka.Error:
			// Ошибка при работе с Kafka
			fmt.Printf("[Push Consumer] Kafka error: %v\n", e)
			time.Sleep(time.Second)
		default:
		}
	}
}
