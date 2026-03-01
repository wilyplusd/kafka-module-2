package main

import (
    "fmt"
    "log"
    "context"

    "github.com/segmentio/kafka-go"
    "kafka-module-2/models"
)

func main() {
    // Создаём Kafka-консьюмера с конфигурацией
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:        []string{"localhost:9092"},
        Topic:          "my-topic",
        GroupID:        "batch-consumer-group",
        MinBytes:       10,   
        MaxWait:        1000, 
        MaxBytes:       10e6,
        CommitInterval: 0,
        StartOffset: kafka.FirstOffset,
    })
    defer reader.Close() // закрываем reader при выходе из main

    // Создаём контекст для FetchMessage
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
        // nil вместо контекста используется для простоты, можно передавать ctx
        if err := reader.CommitMessages(ctx, msgs); err != nil {
            log.Println("Ошибка коммита:", err)
        }
    }
}