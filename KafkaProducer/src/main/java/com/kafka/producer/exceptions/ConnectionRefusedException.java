package com.kafka.producer.exceptions;

public class ConnectionRefusedException extends Exception {
    public ConnectionRefusedException(String exceptionValue) {
        super(exceptionValue);
    }
}
