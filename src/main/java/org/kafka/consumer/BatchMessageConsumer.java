package org.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.kafka.model.Message;
import org.kafka.serializer.MessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BatchMessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(BatchMessageConsumer.class);
    private static final String TOPIC = "practical-topic";
    private static final String GROUP_ID = "batch-group-1"; // Уникальная группа
    private static final int BATCH_SIZE = 10;

    public static void main(String[] args) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());

        // Ручное управление коммитами
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Настройки для получения пачек
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024"); // Минимум 1 КБ данных
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500"); // Макс. ожидание 500 мс

        KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        logger.info("BatchMessageConsumer запущен. Группа: {}. Целевой размер пачки: {} сообщений", GROUP_ID, BATCH_SIZE);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Завершение работы BatchMessageConsumer...");
            consumer.wakeup();
        }));

        try {
            while (true) {
                ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(1000));
                int count = records.count();

                if (count > 0) {
                    logger.info("Получена пачка из {} сообщений", count);

                    for (ConsumerRecord<String, Message> record : records) {
                        try {
                            Message message = record.value();
                            if (message != null) {
                                logger.info("[Batch] Обработано: ключ={}, партиция={}, смещение={}, сообщение={}",
                                        record.key(), record.partition(), record.offset(), message);
                            }
                        } catch (Exception e) {
                            logger.error("Ошибка обработки сообщения в пачке (ключ={}): {}",
                                    record.key(), e.getMessage(), e);
                        }
                    }

                    try {
                        consumer.commitSync();
                        logger.info("Коммит выполнен для {} сообщений", count);
                    } catch (CommitFailedException e) {
                        logger.error("Ошибка коммита пачки: {}", e.getMessage(), e);
                    }
                }
            }
        } catch (WakeupException e) {
            logger.info("Потребитель пробужден для завершения");
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception e) {
                logger.warn("Не удалось выполнить финальный коммит", e);
            }
            consumer.close();
            logger.info("BatchMessageConsumer остановлен");
        }
    }
}
