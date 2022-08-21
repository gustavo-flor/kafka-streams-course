package com.github.gustavoflor.bankbalance.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Balance {
    private String name;
    private BigDecimal totalMoney;
    private String latestTime;

    public Balance() {
        this.totalMoney = BigDecimal.ZERO;
        this.latestTime = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now());
    }

    public Balance(final String name, final double totalMoney, final String latestTime) {
        this.name = name;
        this.totalMoney = BigDecimal.ZERO;
        this.latestTime = latestTime;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getTotalMoney() {
        return totalMoney;
    }

    public String getLatestTime() {
        return latestTime;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTotalMoney(BigDecimal totalMoney) {
        this.totalMoney = totalMoney;
    }

    public void setLatestTime(String latestTime) {
        this.latestTime = latestTime;
    }

    public void addTotalMoney(BigDecimal amount) {
        setTotalMoney(getTotalMoney().add(amount));
    }
}
