package com.kafka.producer.exceptions;

public class WrongPathException extends Exception {
    public WrongPathException(String exceptionValue) {
        super(exceptionValue);
    }
}
