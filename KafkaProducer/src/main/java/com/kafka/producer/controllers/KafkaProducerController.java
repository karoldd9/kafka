package com.kafka.producer.controllers;

import com.kafka.producer.kafka.KafkaServer;
import com.kafka.producer.kafka.ZooKeeper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

@RestController
@RequestMapping("/kafka")
public class KafkaProducerController {

    private final Set<String> topics = new HashSet<>();
    private int counter = 0;

    private KafkaProducer<Integer, String> producer;
    private boolean instanceExists = false;

    private void createInstance() {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Kafka test");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
        instanceExists = true;
    }

    @PostMapping("/produce/{topic}/{value}")
    public String produce(@PathVariable("topic") String topic, @PathVariable("value") String value) {

        if(!ZooKeeper.isRunning) {
            String info = "ZooKeeper "+(ZooKeeper.startZooKeeper() ? "started successfully." : "could not start...");
            System.out.println(info);
        }

        if(!KafkaServer.isRunning) {
            String info = "Kafka server "+(KafkaServer.startKafkaServer() ? "started successfully." : "could not start...");
            System.out.println(info);
        }
        if(!topics.contains(topic)) {
            if(KafkaServer.createTopic(topic)) {
                topics.add(topic);
                System.out.println("Topic created successfully!");
            } else {
                String info = "Could not create topic \""+topic+"\"";
                System.out.println(info);
                return info;
            }
        }

        if(!instanceExists) {
            createInstance();
        }

        producer.send(new ProducerRecord<>(topic, counter++, value));

        return "Successfully created \""+value+"\" in the \""+topic+"\"!";
    }

    @PostMapping("/stopZooKeeper")
    public String stopZooKeeper() {
        return ZooKeeper.stopZookeeper() ? "ZooKeeper has been stopped successfully." : "Could not stop ZooKeeper.";
    }

    @PostMapping("/stopKafkaServer")
    public String stopKafkaServer() {
        return KafkaServer.stopKafkaServer() ? "Kafka Server stopped successfully." : "Could not stop Kafka Server.";
    }
}
