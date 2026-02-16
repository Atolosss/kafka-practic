package org.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.kafka.model.Message;
import org.kafka.serializer.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerApp {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerApp.class);
    private static final String TOPIC = "practical-topic";
    private static final int MESSAGE_COUNT = 100;

    public static void main(String[] args) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        KafkaProducer<String, Message> producer = new KafkaProducer<>(props);
        logger.info("Продюсер запущен. Отправка {} сообщений...", MESSAGE_COUNT);

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String key = "key-" + i;
            Message message = new Message("msg-" + i, "Сообщение #" + i + " от " + System.currentTimeMillis());
            ProducerRecord<String, Message> record = new ProducerRecord<>(TOPIC, key, message);
            try {
                producer.send(record).get();
            } catch (InterruptedException e) {
                logger.error("Продюсер прерван", e);
                break;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RecordTooLargeException) {
                    logger.error("Сообщение слишком большое: {}", message.getId());
                } else {
                    logger.error("Ошибка отправки сообщения {}: {}", message.getId(), e.getMessage());
                }
            }
        }

        producer.flush();
        producer.close(Duration.ofSeconds(10));
        logger.info("Продюсер завершил работу. Отправлено {} сообщений.", MESSAGE_COUNT);
    }
}
