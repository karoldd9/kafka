package com.kafka.consumer.controllers;

import com.kafka.consumer.consumers.KafkaConsumerMessages;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@RequestMapping("/kafka")
public class KafkaConsumerController {

    @GetMapping("/getMessages")
    public List<String> messages() {

        return KafkaConsumerMessages.getMessages();
    }
}
