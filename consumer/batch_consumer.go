package main

import (
	"context"
	"fmt"
	"kafka-module-2/models"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const batchSize = 10

func main() {
	// Создаём kafka-консьюмера
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "my-topic",
		GroupID:        "batch-consumer-group",
		MinBytes:       1024,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,
	})

	// Закрываем reader при выходе из main
	defer reader.Close()

	// Создаём контекст
	ctx := context.Background()

	// Основной цикл для чтения сообщений
	for {
		var batch []kafka.Message

		// Накопление батча
		for len(batch) < batchSize {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				log.Println("Ошибка при fetch:", err)
				break
			}
			batch = append(batch, msg)
		}

		if len(batch) == 0 {
			continue
		}

		// Обработка батча
		for _, msg := range batch {
			message, err := models.Deserialize(msg.Value)
			if err != nil {
				log.Println("Ошибка десериализации:", err)
				continue
			}
			fmt.Println("BatchConsumer получил:", message)
		}

		// Один коммит на всю пачку
		if err := reader.CommitMessages(ctx, batch...); err != nil {
			log.Println("Ошибка коммита батча:", err)
		} else {
			fmt.Printf("Закоммичено сообщений: %d\n", len(batch))
		}
	}
}
