package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"kafka-module-2/models"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Создаём kafka-продюсера
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "my-topic",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: -1,
		MaxAttempts:  10,
		Async:        false,
	})

	// Закрываем writer при выходе из main
	defer writer.Close()

	// Создаём контекст
	ctx := context.Background()

	// Основной бесконечный цикл записи сообщений
	for i := 1; ; i++ {
		// Создаём сообщение с уникальным ID и текстом
		msg := &models.Message{
			ID:      i,
			Content: fmt.Sprintf("Номер сообщения %d", i),
		}

		// Сериализуем сообщение в JSON
		data, err := msg.Serialize()
		if err != nil {
			log.Printf("Ошибка сериализации сообщения ID=%d: %v\n", msg.ID, err)
			continue // не отправляем битое сообщение
		}

		// Отправляем сообщение в Kafka
		err = writer.WriteMessages(ctx, kafka.Message{
			Value: data,
		})
		if err != nil {
			log.Println("Ошибка отправки сообщения:", err)
		}

		// Ждём 500 мс перед отправкой следующего сообщения
		time.Sleep(500 * time.Millisecond)
	}
}
