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

import com.example.query.BankAccountQueryRestController;
import lombok.Data;
import lombok.Value;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.hateoas.ResourceSupport;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static org.springframework.hateoas.mvc.ControllerLinkBuilder.linkTo;
import static org.springframework.hateoas.mvc.ControllerLinkBuilder.methodOn;

/**
 * @author Zoltan Altfatter
 */
@RestController
public class BankAccountCommandRestController {

    private final CommandGateway commandGateway;

    public BankAccountCommandRestController(CommandGateway commandGateway) {
        this.commandGateway = commandGateway;
    }

    @PostMapping("/bank-accounts")
    public ResponseEntity<BankAccountResource> create(@RequestBody BankAccountCreateRequest request) {
        String id = UUID.randomUUID().toString();
        commandGateway.sendAndWait(CreateBankAccountCommand.builder()
                .bankAccountId(id)
                .overdraftLimit(request.getOverdraftLimit())
                .build());

        BankAccountResource bankAccountResource = new BankAccountResource(id);
        return new ResponseEntity<>(bankAccountResource, HttpStatus.CREATED);
    }

    @PostMapping("/bank-accounts/{bankAccountId}/deposits")
    public ResponseEntity<BankAccountResource> deposit(@PathVariable String bankAccountId,
                                                       @RequestBody MoneyAddedRequest request) {
        commandGateway.send(DepositMoneyCommand.builder()
                .bankAccountId(bankAccountId)
                .amount(request.getAmount())
                .build());
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @Data
    static class BankAccountCreateRequest {
        final long overdraftLimit;
    }

    @Data
    static class MoneyAddedRequest {
        final long amount;
    }

    @Value
    static class BankAccountResource extends ResourceSupport {
        public BankAccountResource(String bankAccountId) {
            this.add(linkTo(methodOn(BankAccountQueryRestController.class, "aaa")
                    .findOne(bankAccountId)).withSelfRel());
        }
    }

}


