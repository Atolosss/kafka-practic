package org.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.kafka.model.Message;
import org.kafka.serializer.MessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SingleMessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SingleMessageConsumer.class);
    private static final String TOPIC = "practical-topic";
    private static final String GROUP_ID = "single-group-1"; // Уникальная группа

    public static void main(String[] args) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");

        KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        logger.info("SingleMessageConsumer запущен. Группа: {}", GROUP_ID);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Завершение работы SingleMessageConsumer...");
            consumer.wakeup();
        }));

        try {
            while (true) {
                ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Message> record : records) {
                    try {
                        Message message = record.value();
                        if (message != null) {
                            logger.info("[Single] Обработано: ключ={}, партиция={}, смещение={}, сообщение={}",
                                    record.key(), record.partition(), record.offset(), message);
                        }
                    } catch (Exception e) {
                        logger.error("Ошибка обработки сообщения (ключ={}): {}", record.key(), e.getMessage(), e);
                    }
                }
            }
        } catch (WakeupException e) {
            logger.info("Потребитель пробужден для завершения");
        } finally {
            consumer.close();
            logger.info("SingleMessageConsumer остановлен");
        }
    }
}