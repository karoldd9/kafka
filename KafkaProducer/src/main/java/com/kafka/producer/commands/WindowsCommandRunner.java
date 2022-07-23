package com.kafka.producer.commands;

import com.kafka.producer.exceptions.*;
import com.kafka.producer.kafka.KafkaPath;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class WindowsCommandRunner {
    public static void startZooKeeper() throws Exception{


        ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", KafkaPath.KAFKA_PATH+"\\bin\\windows\\zookeeper-server-start.bat "+ KafkaPath.KAFKA_PATH+"\\config\\zookeeper.properties");
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        String line;
        boolean started = false;
        while(!started) {
            line = bufferedReader.readLine();
            if(line.contains("is not recognized as an internal or external command")) {
                throw new WrongCommandException("Can't execute command");
            }
            if(line.contains("is not recognized as the name of a cmdlet")) {
                throw new WrongCommandException("Not recognized as cmdlet");
            }
            if(line.contains("Address already in use")) {
                throw new AddressAlreadyInUseException("Address is already in use. Probably, service is running.");
            }
            if(line.contains("Error")) {
                throw new Exception("Other error when starting ZooKeeper");
            }
            System.out.println(line);
            if(line.contains("started")) {
                started = true;
            }
        }
        bufferedReader.close();
        System.out.println("Closed command reader.");
    }

    public static void stopZooKeeper() throws Exception{
        ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", KafkaPath.KAFKA_PATH+"\\bin\\windows\\zookeeper-server-stop.bat");
        processBuilder.redirectErrorStream(true);
        processBuilder.start();
    }

    public static void startKafkaServer() throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", KafkaPath.KAFKA_PATH+"\\bin\\windows\\kafka-server-start.bat "+ KafkaPath.KAFKA_PATH+"\\config\\server.properties");
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        String line;
        boolean started = false;
        while (!started) {
            line = bufferedReader.readLine();

            if(line.contains("A Kafka instance in another process or thread is using this directory")) {
                throw new KafkaInstanceInUseException("Probably, this server is already running.");
            }

            if(line.contains("Connection refused: no further information")) {
                throw new ConnectionRefusedException("Connection refused without information. Please check if ZooKeeper is running");
            }

            if(line.contains("Error")) {
                throw new Exception("Other error when starting Kafka Server");
            }

            if(line.contains("started")) {
                started = true;
            }
        }
        bufferedReader.close();
        System.out.println("Closed command reader.");
    }

    public static void stopKafkaServer() throws Exception{
        ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", KafkaPath.KAFKA_PATH+"\\bin\\windows\\kafka-server-stop.bat");
        processBuilder.redirectErrorStream(true);
        processBuilder.start();
    }

    public static void createTopic(String topic) throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", KafkaPath.KAFKA_PATH+"\\bin\\windows\\kafka-topics.bat --create --bootstrap-server localhost:9092 --topic "+topic+" --partitions 1 --replication-factor 1 --config segment.bytes=1000000");
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        String line;
        boolean created = false;
        while(!created) {
            line = bufferedReader.readLine();
            if(line.contains("Broker may not be available")) {
                throw new BrokerNotAvailableException("Broker is not available. Please check if broker is running");
            }

            if(line.contains("already exists")) {
                throw new TopicAlreadyExistsException("Topic '"+topic+"' already exists");
            }

            if(line.contains("Error")) {
                throw new Exception("Other error while creating topic");
            }

            if(line.contains("Created topic")) {
                created = true;
            }
        }
        bufferedReader.close();
        System.out.println("Closed command reader.");
    }
}
