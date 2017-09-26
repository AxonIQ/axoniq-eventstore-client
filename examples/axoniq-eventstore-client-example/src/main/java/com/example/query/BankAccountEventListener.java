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
