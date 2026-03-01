package main

import (
    "fmt"
    "log"
    "time"
    "context"

    "github.com/segmentio/kafka-go"
    "kafka-module-2/models"
)

func main() {
    // Создаём Kafka-продюсера с конфигурацией
    writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers:  []string{"localhost:9092"},
        Topic:    "my-topic",
        Balancer: &kafka.LeastBytes{},
        Async:    false,
    })

    defer writer.Close()

    // Создаём контекст для WriteMessages
    ctx := context.Background()

    // Цикл отправки 50 сообщений
    for i := 1; i <= 50; i++ {
        // Создаём сообщение с уникальным ID и текстом
        msg := &models.Message{
            ID:      i,
            Content: fmt.Sprintf("Номер сообщения %d", i),
        }
        // Сериализуем сообщение в JSON
        data, _ := msg.Serialize()
        fmt.Println("Отправляем:", string(data))

        // Отправляем сообщение в Kafka
        err := writer.WriteMessages(ctx, kafka.Message{
            Value: data,
        })
        if err != nil {
            log.Println("Ошибка отправки сообщения:", err)
        }
        // Ждём 500 мс перед отправкой следующего сообщения
        time.Sleep(500 * time.Millisecond)
    }
}