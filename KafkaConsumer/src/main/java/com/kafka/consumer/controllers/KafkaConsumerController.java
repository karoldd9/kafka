package com.kafka.consumer.controllers;

import com.kafka.consumer.runnable.ConsumerRunnable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@RequestMapping("/kafka")
public class KafkaConsumerController {

    private static Map<String, List<String>> messagesMap = new HashMap<>();
    private Set<ConsumerRunnable> running = new HashSet<>();
    Thread thread;

    public static void addMessage(String topic, String message) {
        System.out.println("Called with "+topic+" "+message);
        List<String> tempMessages = new LinkedList<>();

        if(messagesMap.containsKey(topic)) {
            tempMessages = messagesMap.get(topic);
        }
        tempMessages.add(message);

        messagesMap.put(topic, tempMessages);
        System.out.println(messagesMap);
        /*
        if(messagesMap.containsKey(topic)) {
            messagesMap.get(topic).add(message);
        } else {
            messagesMap.put(topic, List.of(message));
        }
         */
    }

    private void run(String topic, String consumerGroup) {
        boolean alreadyExists = false;
        for(ConsumerRunnable runnable: running) {
            if(runnable.checkTopicAndConsumerGroup(topic, consumerGroup)) {
                alreadyExists = true;
            }
        }

        if(!alreadyExists) {
            ConsumerRunnable consumerRunnable = new ConsumerRunnable(topic, consumerGroup);
            thread = new Thread(consumerRunnable);
            thread.start();

            running.add(consumerRunnable);
        }
    }

    @GetMapping("/hej")
    public String hej() {
        return "Hej";
    }

    @GetMapping("/{topic}/{consumerGroup}")
    public List<String> messages(@PathVariable("topic") String topic, @PathVariable("consumerGroup") String consumerGroup) {

        run(topic, consumerGroup);

        List<String> topicMessages = messagesMap.containsKey(topic) ? messagesMap.get(topic) : List.of();
        System.out.println("List: "+topicMessages);
        System.out.println("Map1: "+messagesMap);

        return topicMessages;
    }
}
