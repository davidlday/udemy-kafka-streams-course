package com.davidlday.udemy.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankBalanceAppTests {

  private TopologyTestDriver testDriver;
  private final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
  private final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
  private final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
  private StringSerializer stringSerializer = new StringSerializer();
  private ConsumerRecordFactory<String, String> recordFactory =
    new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

  @Before
  public void setupTopologyTestDriver() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

    BankBalanceApp bankBalanceApp = new BankBalanceApp();
    Topology topology = bankBalanceApp.createTopology();

    testDriver = new TopologyTestDriver(topology, config);
  }

  @After
  public void closeTestDriver() {
    testDriver.close();
  }

  public ProducerRecord<String, JsonNode> updateBalance(String key, String value) {
    testDriver.pipeInput((recordFactory.create("bank-transactions", key, value)));
    return testDriver.readOutput("account-balances", new StringDeserializer(),  new JsonDeserializer());
  }

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

    ProducerRecord<String, JsonNode> balanceRecord = updateBalance(transaction.get("name").asText(), transaction.toString());;
    JsonNode balance = balanceRecord.value();

    Long balanceEpoch = Instant.parse(balance.get("lastUpdated").asText()).toEpochMilli();
    Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();

    assertEquals("John", balanceRecord.key());
    assertEquals(1, balance.get("transactionCount").asInt());
    assertEquals(100, balance.get("balance").asInt());
    assertEquals(transactionEpoch, balanceEpoch);

    System.out.println(balance);

    now = Instant.now();

    transaction.put("name", "John");
    transaction.put("amount", 150);
    transaction.put("time", now.toString());

    balanceRecord = updateBalance(transaction.get("name").asText(), transaction.toString());
    balance = balanceRecord.value();

    balanceEpoch = Instant.parse(balance.get("lastUpdated").asText()).toEpochMilli();
    transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();

    assertEquals("John", balanceRecord.key());
    assertEquals(2, balance.get("transactionCount").asInt());
    assertEquals(250, balance.get("balance").asInt());
    assertEquals(transactionEpoch, balanceEpoch);

    System.out.println(balance);
  }

}
