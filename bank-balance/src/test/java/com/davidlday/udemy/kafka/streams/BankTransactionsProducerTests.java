package com.davidlday.udemy.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankTransactionsProducerTests {

  @Test
  public void generateTransaction(){
    ProducerRecord<String, String> record = BankTransactionsProducer.generateTransaction("John");
    String key = record.key();
    String value = record.value();

    assertEquals(key, "John");

    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode node = mapper.readTree(value);
      assertEquals(node.get("name").asText(), "John");
      assertTrue("Amount must be greater than " + BankTransactionsProducer.minDollars.toString(), node.get("amount").asInt() > BankTransactionsProducer.minDollars);
      assertTrue("Amount must be less than " + BankTransactionsProducer.maxDollars.toString(), node.get("amount").asInt() < BankTransactionsProducer.maxDollars);
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println(value);

  }


}
