package com.kafka.producer.exceptions;

public class BrokerNotAvailableException extends Exception{
    public BrokerNotAvailableException(String exceptionValue) {
        super(exceptionValue);
    }
}
