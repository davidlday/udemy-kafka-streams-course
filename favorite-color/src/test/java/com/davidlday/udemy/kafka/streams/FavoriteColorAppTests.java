package com.davidlday.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class FavoriteColorAppTests {

  TopologyTestDriver testDriver;
  StringSerializer stringSerializer = new StringSerializer();
  ConsumerRecordFactory<String, String> recordFactory =
    new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

  @Before
  public void setupTopologyTestDriver() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "testdriver:1234");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    FavoriteColorApp favoriteColorApp = new FavoriteColorApp();
    Topology topology = favoriteColorApp.createTopology();

    testDriver = new TopologyTestDriver(topology, config);
  }

  @After
  public void closeTestDriver() {
    testDriver.close();
  }

  public void streamInputRecord(String value) {
    testDriver.pipeInput((recordFactory.create("favorite-color-input", null, value)));
  }

  public ProducerRecord<String, Long> readOutputRecord() {
    return testDriver.readOutput("favorite-color-output", new StringDeserializer(), new LongDeserializer());
  }

  @Test
  public void favoriteColorTopologyTest() {
    streamInputRecord("stephane,blue");
    OutputVerifier.compareKeyValue(readOutputRecord(), "blue", 1L);
    assertEquals(readOutputRecord(), null);

    streamInputRecord("john,green");
    OutputVerifier.compareKeyValue(readOutputRecord(), "green", 1L);
    assertEquals(readOutputRecord(), null);

    streamInputRecord("stephane,red");
    OutputVerifier.compareKeyValue(readOutputRecord(), "blue", 0L);
    OutputVerifier.compareKeyValue(readOutputRecord(), "red", 1L);
    assertEquals(readOutputRecord(), null);

    streamInputRecord("alice,red");
    OutputVerifier.compareKeyValue(readOutputRecord(), "red", 2L);
    assertEquals(readOutputRecord(), null);
  }

  public void colorNotValidTest() {
    streamInputRecord("david,black");
    assertEquals(readOutputRecord(), null);
  }

}
