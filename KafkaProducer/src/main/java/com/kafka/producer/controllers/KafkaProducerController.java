package com.kafka.producer.controllers;

import com.kafka.producer.kafka.ZooKeeper;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaProducerController {

    @PostMapping("/produce/{value}")
    public String produce(@PathVariable("value") String value) {

        if(!ZooKeeper.isRunning) {
            System.out.println("ZooKeeper "+(ZooKeeper.startZooKeeper() ? "started successfully." : "could not start..."));
        }

        return "Some of the services didn't start. Please wait a few seconds and send "+value+" again.";
    }

    @PostMapping("/stopZooKeeper")
    public String stopZooKeeper() {
        return ZooKeeper.stopZookeeper() ? "ZooKeeper has been stopped successfully." : "Could not stop ZooKeeper";
    }
}
