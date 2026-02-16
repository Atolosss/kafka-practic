# Практическая работа №2: Kafka Consumers и Гарантии Доставки

## 📌 Требования
- Java 11+
- Docker + Docker Compose
- Gradle (или используем gradlew)

## 🚀 Запуск проекта

### 1. Запустить Kafka кластер
docker-compose up -d

### 2. Создать топик
docker exec kafka1 kafka-topics --create \
  --topic practical-topic \
  --partitions 3 \
  --replication-factor 2 \
  --bootstrap-server localhost:9092

### 3. Собрать проект
./gradlew clean build

### 4. Запустить компоненты (в отдельных терминалах)
Продюсер:
./gradlew run --args='producer'
ИЛИ
java -jar build/libs/kafka-java-gradle-1.0-producer.jar

SingleMessageConsumer:
java -cp build/libs/kafka-java-gradle-1.0.jar ru.practical.consumer.SingleMessageConsumer

BatchMessageConsumer (запустить 2 раза для проверки параллельности):
java -cp build/libs/kafka-java-gradle-1.0.jar ru.practical.consumer.BatchMessageConsumer

### 5. Проверить логи и консоль.