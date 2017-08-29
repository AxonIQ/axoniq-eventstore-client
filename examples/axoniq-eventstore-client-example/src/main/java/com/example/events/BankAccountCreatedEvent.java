package com.example.events;

import lombok.Data;

/**
 * @author Zoltan Altfatter
 */
@Data
public class BankAccountCreatedEvent {

    private final String id;
    private final long overdraftLimit;

}
