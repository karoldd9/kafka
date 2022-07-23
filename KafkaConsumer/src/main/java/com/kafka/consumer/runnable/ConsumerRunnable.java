package com.kafka.consumer.runnable;

import com.kafka.consumer.controllers.KafkaConsumerController;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class ConsumerRunnable implements Runnable{

    private KafkaConsumer<Integer, String> kafkaConsumer;
    private final String topic;
    private final String consumerGroup;
    private boolean interrupted = false;

    public ConsumerRunnable(String topic, String consumerGroup) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    public void interrupt() {
        interrupted = true;
    }

    public boolean checkTopicAndConsumerGroup(String topic, String consumerGroup) {
        System.out.printf("Checking samenity");
        if(this.topic.equals(topic) && this.consumerGroup.equals(consumerGroup))
            return true;

        return false;
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        if(kafkaConsumer == null) {
            kafkaConsumer = new KafkaConsumer<>(properties);
        }

        kafkaConsumer.subscribe(Arrays.asList(topic));

        while(!interrupted) {
            ConsumerRecords<Integer, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord record: records) {
                KafkaConsumerController.addMessage(record.topic(), (String) record.value());
                System.out.println("Adding: "+record.value());
            }
        }
    }
}
