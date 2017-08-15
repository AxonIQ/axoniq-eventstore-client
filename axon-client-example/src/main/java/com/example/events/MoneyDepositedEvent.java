package com.example.events;

import lombok.Data;

/**
 * @author Zoltan Altfatter
 */
@Data
public class MoneyDepositedEvent {

    private final String bankAccountId;
    private final long amount;

}
