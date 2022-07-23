package com.kafka.producer.kafka;

import com.kafka.producer.commands.WindowsCommandRunner;
import com.kafka.producer.exceptions.BrokerNotAvailableException;
import com.kafka.producer.exceptions.ConnectionRefusedException;
import com.kafka.producer.exceptions.KafkaInstanceInUseException;
import com.kafka.producer.exceptions.TopicAlreadyExistsException;

public class KafkaServer {

    public static boolean isRunning = false;

    public static boolean startKafkaServer() {
        try {
            WindowsCommandRunner.startKafkaServer();
        } catch (KafkaInstanceInUseException e) {
            e.printStackTrace();
            isRunning = true;
            return true;
        }
        catch (ConnectionRefusedException e) {
            e.printStackTrace();
            System.out.println("Trying to run ZooKeeper...");
            if(ZooKeeper.startZooKeeper()) {
                try {
                    WindowsCommandRunner.startKafkaServer();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return false;
                }
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        isRunning = true;
        return true;
    }

    public static boolean stopKafkaServer() {
        try {
            WindowsCommandRunner.stopKafkaServer();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Could not stop Kafka Server.");
            return false;
        }
        System.out.println("Kafka Server stopped successfully.");
        return true;
    }

    public static boolean createTopic(String topic) {
        try {
            WindowsCommandRunner.createTopic(topic);
        } catch (TopicAlreadyExistsException e) {
            e.printStackTrace();
            return true;
        } catch (BrokerNotAvailableException e) {
            e.printStackTrace();
            System.out.println("Trying to start broker...");
            try {
                startKafkaServer();
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("Broker didn't start...");
                return false;
            }
            System.out.println("Broker started, trying to create topic again...");
            try {
                WindowsCommandRunner.createTopic(topic);
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("Couldn't create topic");
                return false;
            }
            System.out.println("Topic created!");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
