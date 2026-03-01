package main

import (
    "fmt"
    "log"
    "context"

    "github.com/segmentio/kafka-go"
    "kafka-module-2/models"
)

func main() {
     // Создаём Kafka-консьюмера для чтения сообщений по одному
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:  []string{"localhost:9092"},
        Topic:    "my-topic",
        GroupID:  "single-consumer-group",
        MinBytes: 1,
        MaxBytes: 10e6,
    })
    defer reader.Close()

    // Создаём контекст для ReadMessage
    ctx := context.Background()

    // Основной бесконечный цикл чтения сообщений
    for {
        // Читаем одно сообщение из Kafka
        msg, err := reader.ReadMessage(ctx)
        if err != nil {
            log.Println("Ошибка при чтении:", err)
            continue
        }
        // Десериализуем сообщение из JSON в структуру Message
        message, err := models.Deserialize(msg.Value)
        if err == nil {
            fmt.Println("SingleConsumer получил:", message)
        }
        // SingleConsumer читает по одному сообщению,
        // оффсет коммитится автоматически (по умолчанию)
    }
}