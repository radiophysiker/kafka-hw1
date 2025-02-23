package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/radiophysiker/kafka-hw1/models"
)

// Consumer для
func main() {
	// Config for Kafka consumer pull
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094,localhost:9095",
		"group.id":          "pull-consumer-group",
		"auto.offset.reset": "earliest",
		// Отключаем автоматический коммит оффсетов
		"enable.auto.commit": false,
		// Минимальный объём данных (в байтах), который консьюмер должен получить за один запрос к брокеру Kafka.
		"fetch.min.bytes": 1024,
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

	fmt.Println("[Pull Consumer] Starting...")
	// Пуллинг сообщений
	for {
		// Параметр - таймаут ожидания сообщения
		ev := c.Poll(1000)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			// Получено новое сообщение
			msg, err := models.Deserialize(e.Value)
			if err != nil {
				fmt.Printf("[Pull Consumer] Deserialization error: %v\n", err)
				continue
			}
			fmt.Printf("[Pull Consumer] Received message: ID=%d, Content=%s\n", msg.ID, msg.Content)

			// Вручную коммитим полученный offset
			_, err = c.Commit()
			if err != nil {
				fmt.Printf("[Pull Consumer] Commit error: %v\n", err)
			}

		case kafka.Error:
			// Ошибка при работе с Kafka
			fmt.Printf("[Pull Consumer] Kafka error: %v\n", e)
			time.Sleep(time.Second)
		default:
		}
	}
}
