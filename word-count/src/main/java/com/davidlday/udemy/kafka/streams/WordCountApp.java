package com.davidlday.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

  public static void main(String[] args) {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> wordCountInput = builder.stream("word-count-input");

    KTable<String, Long> wordCounts;
    wordCounts = wordCountInput
      .mapValues((ValueMapper<String, String>) String::toLowerCase)
      .flatMapValues(value -> Arrays.asList(value.split(" ")))
      .selectKey((key, word) -> word)
      .groupByKey()
      .count(Materialized.as("Counts"));

    wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, config);
    streams.start();

    // Print the topology
    System.out.println(streams.toString());

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

}
