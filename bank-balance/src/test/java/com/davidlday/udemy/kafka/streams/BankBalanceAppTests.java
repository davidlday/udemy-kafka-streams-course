package com.davidlday.udemy.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankBalanceAppTests {
  @Test
  public void updateBalance() {
    ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
    initialBalance.put("transactionCount", 0);
    initialBalance.put("balance", 0);
    initialBalance.put("lastUpdated", Instant.ofEpochMilli(0L).toString());

    Instant now = Instant.now();

    ObjectNode transaction = JsonNodeFactory.instance.objectNode();
    transaction.put("name", "John");
    transaction.put("amount", 100);
    transaction.put("time", now.toString());

    JsonNode balance = BankBalanceApp.updateBalance(transaction, initialBalance);

    Long balanceEpoch = Instant.parse(balance.get("lastUpdated").asText()).toEpochMilli();
    Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();

    assertEquals(1, balance.get("transactionCount").asInt());
    assertEquals(100, balance.get("balance").asInt());
    assertEquals(transactionEpoch, balanceEpoch);

    System.out.println(balance);

    now = Instant.now();

    transaction.put("name", "John");
    transaction.put("amount", 150);
    transaction.put("time", now.toString());

    balance = BankBalanceApp.updateBalance(transaction, balance);

    balanceEpoch = Instant.parse(balance.get("lastUpdated").asText()).toEpochMilli();
    transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();

    assertEquals(2, balance.get("transactionCount").asInt());
    assertEquals(250, balance.get("balance").asInt());
    assertEquals(transactionEpoch, balanceEpoch);

    System.out.println(balance);
  }

}
