package com.kafka.producer.commands;

import com.kafka.producer.exceptions.AddressAlreadyInUseException;
import com.kafka.producer.exceptions.WrongCommandException;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class WindowsCommandRunner {
    public static void startZooKeeper(String command) throws Exception{

        if(!command.contains("zookeeper-server-start")) {
            throw new WrongCommandException("This method was created only for ZOOKEEPER STARTING. Please use other method for your command!");
        }
        ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", command);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        String line;
        boolean started = false;
        while(!started) {
            line = bufferedReader.readLine();
            if(line.contains("is not recognized as an internal or external command")) {
                throw new WrongCommandException("Can't execute command \""+command+"\"");
            }
            if(line.contains("is not recognized as the name of a cmdlet")) {
                throw new WrongCommandException("Not recognized as cmdlet: \""+command+"\"");
            }
            if(line.contains("Address already in use")) {
                throw new AddressAlreadyInUseException("Address is already in use. Probably, service is running.");
            }
            System.out.println(line);
            if(line.contains("started")) {
                started = true;
            }
        }
        bufferedReader.close();
        System.out.println("Closed command reader.");
    }

    public static void stopZooKeeper(String command) throws Exception{
        if(!command.contains("zookeeper-server-stop")) {
            throw new WrongCommandException("This method was created only for ZOOKEEPER STOPPING. Please use other method for your command!");
        }

        ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", command);
        processBuilder.redirectErrorStream(true);
        processBuilder.start();
    }
}
