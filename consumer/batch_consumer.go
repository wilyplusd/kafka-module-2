package main

import (
    "fmt"
    "log"
    "context"

    "github.com/segmentio/kafka-go"
    "kafka-module-2/models"
)

func main() {
    // Создаём kafka-консьюмера
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:        []string{"localhost:9092"},
        Topic:          "my-topic",
        GroupID:        "batch-consumer-group",
        MinBytes:       1024,   
        MaxBytes:       10e6,
        CommitInterval: 0,
    })

    // Закрываем reader при выходе из main
    defer reader.Close()

    // Создаём контекст
    ctx := context.Background()

    // Основной цикл для чтения сообщений
    for {
        // FetchMessage получает одно сообщение (или блокирует до получения)
        msgs, err := reader.FetchMessage(ctx)
        if err != nil {
            log.Println("Ошибка при fetch:", err)
            continue
        }

        // Десериализуем полученное сообщение из JSON в структуру Message
        message, err := models.Deserialize(msgs.Value)
        if err == nil {
            fmt.Println("BatchConsumer получил:", message)
        }

        // Ручной коммит оффсета после обработки сообщения
        if err := reader.CommitMessages(ctx, msgs); err != nil {
            log.Println("Ошибка коммита:", err)
        }
    }
}