package com.kafka.consumer.controllers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.*;

@RestController
@RequestMapping("/kafka")
public class KafkaConsumerController {




    @GetMapping("/hej")
    public String hej() {
        return "Hej";
    }

    @GetMapping("/{topic}/{consumerGroup}")
    public List<String> messages(@PathVariable("topic") String topic, @PathVariable("consumerGroup") String consumerGroup) {


        return pollData(topic, consumerGroup);
    }


    private KafkaConsumer<Integer, String> kafkaConsumer;

    private List<String> pollData(String topic, String consumerGroup) {
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

        List<String> result = new LinkedList<>();

        ConsumerRecords<Integer, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
        for(ConsumerRecord record: records) {
            result.add(""+record.value());
            System.out.println("Adding: "+record.value());
        }
        return result;
    }
}
