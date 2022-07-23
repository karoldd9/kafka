package com.kafka.producer.kafka;

import com.kafka.producer.commands.WindowsCommandRunner;
import com.kafka.producer.exceptions.AddressAlreadyInUseException;

public class ZooKeeper {
    public static boolean isRunning = false;

    public static boolean startZooKeeper() {
        try {
            WindowsCommandRunner.startZooKeeper(KafkaPath.KAFKA_PATH+"\\bin\\windows\\zookeeper-server-start.bat "+ KafkaPath.KAFKA_PATH+"\\config\\zookeeper.properties");
        } catch (AddressAlreadyInUseException e) {
            e.printStackTrace();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        isRunning = true;
        return true;
    }

    public static boolean stopZookeeper() {
        try {
            WindowsCommandRunner.stopZooKeeper(KafkaPath.KAFKA_PATH+"\\bin\\windows\\zookeeper-server-stop.bat");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Could not stop ZooKeeper");
            return false;
        }
        System.out.println("ZooKeeper has been stopped successfully");
        return true;
    }
}
