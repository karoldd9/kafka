package com.kafka.producer.exceptions;

public class WrongCommandException extends Exception{
    public WrongCommandException(String exceptionValue) {
        super(exceptionValue);
    }
}
