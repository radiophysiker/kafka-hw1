# README.md

В этом проекте реализовано приложение на языке Go

## Структура проекта

Проект организован следующим образом:

- **models**
  Описано модель сообщения для producer и consumer

- **producer**
  Генерация сообщений и отправка их в Kafka

- **consumer_pull**
  Consumer использует pull-модель (с использованием пуллинга) для регулярной проверки сообщений

- **consumer_push**
  Consumer реагирует на сообщения сразу после их поступления и считывать их

## Инструкция по запуску приложения

1. Развернуть docker

  ```bash
   docker compose up
   ```

2. Создать топики

   ```bash
   kafka-topics.sh --create --bootstrap-server localhost:9094 --replication-factor 2 --partitions 3 --topic my-topic
   ```

3. **Запустить producer**

   ```bash
   go run ./producer/producer.go
   ```

4. **Запустить consumer_push**

   ```bash
   go run ./consumer_push/consumer_push.go
   ```

5. **Запустить consumer_pull**

   ```bash
   go run ./consumer_pull/consumer_pull.go
   ```
