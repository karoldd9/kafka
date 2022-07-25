package com.kafka.consumer.consumers;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class KafkaConsumerMessages {

    private static List<String> messages =  new LinkedList<>();

    public static List<String> getMessages() {
        return messages;
    }

    @KafkaListener(topics = "Topic", groupId = "KafkaConsumer")
    public void consume(String message) {
        messages.add(message);
    }
}
