package com.example.query;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Zoltan Altfatter
 */
public interface BankAccountQueryRepository extends JpaRepository<BankAccount, String> {
}
