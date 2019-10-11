package com.davidlday.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;


import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp {

  public static void main(String[] args) {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KStream<String, String> favoriteColorInput = streamsBuilder.stream("favorite-color-input");

    KStream<String, String> usersAndColors;
    usersAndColors = favoriteColorInput
      .filter((key, value) -> value.contains(","))
      .selectKey((key, value) -> value.split(",")[0].toLowerCase())
      .mapValues(value -> value.split(",")[1].toLowerCase())
      .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));

    usersAndColors.to("favorite-color-intermediary");

    KTable<String, String> usersAndColorsTable = streamsBuilder.table("favorite-color-intermediary");

    Serde<String> stringSerde = Serdes.String();
    Serde<Long> longSerde = Serdes.Long();

    KTable<String, Long> colorCounts;
    colorCounts = usersAndColorsTable
      .groupBy((user, color) -> new KeyValue<>(color, color))
      .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColor")
        .withKeySerde(stringSerde)
        .withValueSerde(longSerde)
      );

    colorCounts.toStream().to("favorite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

    Topology topology = streamsBuilder.build();
    KafkaStreams streams = new KafkaStreams(topology, config);
    streams.start();

    // Print the topology
    System.out.println(streams.toString());

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

}
