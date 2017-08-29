package com.example.command;

import lombok.Builder;
import lombok.Data;
import org.axonframework.commandhandling.TargetAggregateIdentifier;

import javax.validation.constraints.Min;

/**
 * @author Zoltan Altfatter
 */
@Data
@Builder
public class CreateBankAccountCommand {

    @TargetAggregateIdentifier
    private String bankAccountId;

    @Min(value = 0, message = "Overdraft limit must not be less than zero")
    private long overdraftLimit;

}
