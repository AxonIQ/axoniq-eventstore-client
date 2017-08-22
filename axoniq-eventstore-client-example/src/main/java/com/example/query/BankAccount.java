package com.example.query;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Zoltan Altfatter
 */
@Data
@Entity
@Table(name = "bank_accounts")
public class BankAccount {

    @Id
    private String id;

    private long overdraftLimit;
    private long balanceInCents;

}
