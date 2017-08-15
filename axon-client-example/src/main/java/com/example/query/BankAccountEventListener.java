package com.example.query;

import com.example.events.BankAccountCreatedEvent;
import com.example.events.MoneyDepositedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.EventHandler;
import org.springframework.stereotype.Component;

/**
 * @author Zoltan Altfatter
 */
@Component
@Slf4j
public class BankAccountEventListener {

    private final BankAccountQueryRepository repository;

    public BankAccountEventListener(BankAccountQueryRepository repository) {
        this.repository = repository;
    }

    @EventHandler
    public void on(BankAccountCreatedEvent event) {
        log.info("event handler handling event {}", event);
        BankAccount bankAccount = new BankAccount();
        bankAccount.setId(event.getId());
        bankAccount.setOverdraftLimit(event.getOverdraftLimit());

        repository.save(bankAccount);
    }

    @EventHandler
    public void on(MoneyDepositedEvent event) {
        log.info("event handler handling event {}", event);
        BankAccount bankAccount = repository.findOne(event.getBankAccountId());
        bankAccount.setBalanceInCents(bankAccount.getBalanceInCents() + event.getAmount());

        repository.save(bankAccount);
    }
}
