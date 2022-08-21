package com.github.gustavoflor.bankbalance.model;

import java.math.BigDecimal;

public class Transaction {
    private String name;
    private BigDecimal amount;
    private String occurredAt;

    public Transaction() {
    }

    public Transaction(final String name, final BigDecimal amount, final String occurredAt) {
        this.name = name;
        this.amount = amount;
        this.occurredAt = occurredAt;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getOccurredAt() {
        return occurredAt;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public void setOccurredAt(String occurredAt) {
        this.occurredAt = occurredAt;
    }
}
