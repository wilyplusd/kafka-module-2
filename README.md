# Запуск и проверка

1. Запустить локальный кластер Kafka.

```bash
docker-compose up
```

2. Запустить продюсер.

```bash
go run producer/producer.go
```

3. Запустить консьюмеров.

```bash
go run consumer/single_consumer.go
go run consumer/batch_consumer.go
```

4. Наблюдать в консоли запись и чтение сообщений.
