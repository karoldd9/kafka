package com.kafka.producer.exceptions;

public class AddressAlreadyInUseException extends Exception{
    public AddressAlreadyInUseException(String exceptionValue) {
        super(exceptionValue);
    }
}
