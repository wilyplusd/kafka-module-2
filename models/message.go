package models

import (
    "encoding/json"
    "fmt"
)

// Message — структура для сообщений Kafka
type Message struct {
    ID      int    `json:"id"`
    Content string `json:"content"`
}

// Serialize — метод для преобразования структуры Message в JSON []byte
func (m *Message) Serialize() ([]byte, error) {
    data, err := json.Marshal(m)
    if err != nil {
        fmt.Println("Ошибка сериализации:", err)
        return nil, err
    }
    return data, nil
}

// Deserialize — функция для преобразования JSON []byte обратно в структуру Message
func Deserialize(data []byte) (*Message, error) {
    var msg Message
    err := json.Unmarshal(data, &msg)
    if err != nil {
        fmt.Println("Ошибка десериализации:", err)
        return nil, err
    }
    return &msg, nil
}