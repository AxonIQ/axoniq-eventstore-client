/*
 * Copyright (c) 2017. AxonIQ
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.command;

import com.example.events.BankAccountCreatedEvent;
import com.example.events.MoneyDepositedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.spring.stereotype.Aggregate;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;


/**
 * @author Zoltan Altfatter
 */
@Aggregate
@Slf4j
public class BankAccountAggregate {

    @AggregateIdentifier
    private String id;

    private long overdraftLimit;
    private long balanceInCents;

    // needed otherwise Spring cannot wire: No qualifying bean of type 'com.com.example.command.CreateBankAccountCommand'
    private BankAccountAggregate() {
        log.info("creating aggregate instance");
    }

    @CommandHandler
    public BankAccountAggregate(CreateBankAccountCommand command) {
        log.info("received command {}", command);
        apply(new BankAccountCreatedEvent(command.getBankAccountId(), command.getOverdraftLimit()));
    }

    @CommandHandler
    public void deposit(DepositMoneyCommand command) {
        log.info("received command {}", command);
        apply(new MoneyDepositedEvent(id, command.getAmount()));
    }

    // invoked either by the apply method or by the loading from database when re-creating the aggregate
    @EventSourcingHandler
    public void on(BankAccountCreatedEvent event) {
        log.info("event sourcing handler handling event {}", event);
        this.id = event.getId();
        this.overdraftLimit = event.getOverdraftLimit();
        this.balanceInCents = 0;
    }

    @EventSourcingHandler
    public void on(MoneyDepositedEvent event) {
        log.info("event sourcing handler handling event {}", event);
        this.balanceInCents += event.getAmount();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getOverdraftLimit() {
        return overdraftLimit;
    }

    public void setOverdraftLimit(long overdraftLimit) {
        this.overdraftLimit = overdraftLimit;
    }

    public long getBalanceInCents() {
        return balanceInCents;
    }

    public void setBalanceInCents(long balanceInCents) {
        this.balanceInCents = balanceInCents;
    }
}
