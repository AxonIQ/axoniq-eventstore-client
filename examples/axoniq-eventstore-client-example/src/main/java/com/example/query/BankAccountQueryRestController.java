package com.example.query;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author Zoltan Altfatter
 */
@RestController
public class BankAccountQueryRestController {

    private final BankAccountQueryRepository repository;

    public BankAccountQueryRestController(BankAccountQueryRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/bank-accounts")
    public List<BankAccount> findAll() {
        return repository.findAll();
    }

    @GetMapping("/bank-accounts/{id}")
    public ResponseEntity<BankAccount> findOne(@PathVariable String id) {
        BankAccount bankAccount = repository.findOne(id);
        return bankAccount == null ? new ResponseEntity<>(HttpStatus.NOT_FOUND) :
                new ResponseEntity<>(bankAccount, HttpStatus.OK);
    }


}
