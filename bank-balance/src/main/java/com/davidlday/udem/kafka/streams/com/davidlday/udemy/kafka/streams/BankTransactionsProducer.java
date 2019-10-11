package com.davidlday.udem.kafka.streams.com.davidlday.udemy.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {

  static Integer minDollars = 0;
  static Integer maxDollars = 9999;
  static String[] customers = {"John", "Mary", "David", "Logan", "Dylan", "Denna"};
  static Integer msgPerSecond = 100;

  public static void main(String[] args) {
    Properties config = new Properties();

    config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // producer acks -> "all" is the strongest guarantee
    config.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
    config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
    // leverage idempotent producer
    config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    KafkaProducer<String, String> producer = new KafkaProducer<>(config);

    int counter = 0;
    while (true) {
      System.out.println("Transaction batch: " + counter);
      for (String customer : customers) {
        try {
          ProducerRecord<String, String> record = generateTransaction(customer);
          producer.send(record);
          Thread.sleep((1 / customers.length) * (1000 / msgPerSecond) );
          counter += 1;
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }

  private static ProducerRecord<String, String> generateTransaction(String name) {
    // JSON object
    ObjectNode transaction = JsonNodeFactory.instance.objectNode();
    // Random amount between minDollars and maxDollars
    Integer amt = ThreadLocalRandom.current().nextInt(minDollars, maxDollars);
    // Timestamp in UTC
    Instant now = Instant.now();
    transaction.put("name", name);
    transaction.put("amount", amt);
    transaction.put("time", now.toString());
    return new ProducerRecord<String, String>("bank-transactions", name, transaction.toString());
  }

}
