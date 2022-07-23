package com.kafka.producer.exceptions;

public class TopicAlreadyExistsException extends Exception {
    public TopicAlreadyExistsException(String exceptionValue) {
        super(exceptionValue);
    }
}
