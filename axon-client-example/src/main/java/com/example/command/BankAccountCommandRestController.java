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


