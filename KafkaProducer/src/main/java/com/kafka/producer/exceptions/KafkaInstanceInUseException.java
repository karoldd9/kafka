package com.kafka.producer.exceptions;

public class KafkaInstanceInUseException extends Exception {
    public KafkaInstanceInUseException(String exceptionValue) {
        super(exceptionValue);
    }
}
