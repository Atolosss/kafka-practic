package org.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.kafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MessageSerializer implements Serializer<Message> {
    private static final Logger logger = LoggerFactory.getLogger(MessageSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Message data) {
        if (data == null) {
            return null;
        }
        try {
            String json = objectMapper.writeValueAsString(data);
            return json.getBytes();
        } catch (Exception e) {
            logger.error("Ошибка сериализации сообщения: " + data, e);
            return null;
        }
    }

    @Override
    public void close() {
    }
}
