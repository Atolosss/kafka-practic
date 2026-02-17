package org.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.kafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MessageDeserializer implements Deserializer<Message> {
    private static final Logger logger = LoggerFactory.getLogger(MessageDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Message deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            Message message = objectMapper.readValue(data, Message.class);
            return message;
        } catch (Exception e) {
            logger.error("Ошибка десериализации данных: " + new String(data), e);
            return null;
        }
    }

    @Override
    public void close() {
    }
}
