package com.example.command;

import lombok.Builder;
import lombok.Data;
import org.axonframework.commandhandling.TargetAggregateIdentifier;

/**
 * @author Zoltan Altfatter
 */
@Data
@Builder
public class DepositMoneyCommand {

    @TargetAggregateIdentifier
    private String bankAccountId;
    private long amount;
}
