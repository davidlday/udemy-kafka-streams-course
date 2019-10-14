package com.davidlday.udemy.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceApp {

  public Topology createTopology() {
    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
    initialBalance.put("transactionCount", 0);
    initialBalance.put("balance", 0);
    initialBalance.put("lastUpdated", Instant.ofEpochMilli(0L).toString());

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KStream<String, JsonNode> transactionsInput = streamsBuilder.stream("bank-transactions",
      Consumed.with(Serdes.String(), jsonSerde));

    KTable<String, JsonNode> accountBalances = transactionsInput
      .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
      .aggregate(
        () -> initialBalance,
        (aggregateKey, transaction, currentBalance) -> updateBalance(transaction, currentBalance),
        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("account-balance-aggregator")
          .withKeySerde(Serdes.String())
          .withValueSerde(jsonSerde)
      );

    accountBalances.toStream().to("account-balances", Produced.with(Serdes.String(), jsonSerde));

    return streamsBuilder.build();
  }

  public static void main(String[] args) {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); // Disabled for demonstration only
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

    BankBalanceApp bankBalanceApp = new BankBalanceApp();

    // Start the streams app
    Topology topology = bankBalanceApp.createTopology();
    KafkaStreams streams = new KafkaStreams(topology, config);
    streams.cleanUp();
    streams.start();

    // Print the topology
    System.out.println(streams.toString());

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

  public static JsonNode updateBalance(JsonNode transaction, JsonNode currentBalance) {
    ObjectNode newBalance = JsonNodeFactory.instance.objectNode();

    newBalance.put("transactionCount", currentBalance.get("transactionCount").asInt() + 1);
    newBalance.put("balance", currentBalance.get("balance").asInt()
      + transaction.get("amount").asInt());

    Long balanceEpoch = Instant.parse(currentBalance.get("lastUpdated").asText()).toEpochMilli();
    Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
    Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
    newBalance.put("lastUpdated", newBalanceInstant.toString());

    return newBalance;
  }

}
