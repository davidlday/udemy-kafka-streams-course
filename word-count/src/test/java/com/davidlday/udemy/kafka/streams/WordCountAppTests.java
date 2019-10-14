package com.davidlday.udemy.kafka.streams;

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

public class WordCountAppTests {

  private TopologyTestDriver testDriver;
  private StringSerializer stringSerializer = new StringSerializer();
  private ConsumerRecordFactory<String, String> recordFactory =
    new ConsumerRecordFactory<String, String>(stringSerializer, stringSerializer);

  @Before
  public void setupTopologyTestDriver() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "testdriver:1234");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    WordCountApp wordCountApp = new WordCountApp();
    Topology topology = wordCountApp.createTopology();

    testDriver = new TopologyTestDriver(topology, config);

  }

  @After
  public void closeTestDriver() {
    testDriver.close();
  }

  public void streamInputRecord(String value) {
    testDriver.pipeInput((recordFactory.create("word-count-input", null, value)));
  }

  public ProducerRecord<String, Long> readOutputRecord() {
    return testDriver.readOutput("word-count-output", new StringDeserializer(), new LongDeserializer());
  }

  @Test
  public void wordCountTopologyTest() {
    streamInputRecord("testing Kafka Streams");
    OutputVerifier.compareKeyValue(readOutputRecord(), "testing", 1L);
    OutputVerifier.compareKeyValue(readOutputRecord(), "kafka", 1L);
    OutputVerifier.compareKeyValue(readOutputRecord(), "streams", 1L);
    assertEquals(readOutputRecord(), null);

    streamInputRecord("testing Kafka again");
    OutputVerifier.compareKeyValue(readOutputRecord(), "testing", 2L);
    OutputVerifier.compareKeyValue(readOutputRecord(), "kafka", 2L);
    OutputVerifier.compareKeyValue(readOutputRecord(), "again", 1L);
    assertEquals(readOutputRecord(), null);
  }

  @Test
  public void lowerCaseWordsTest() {
    streamInputRecord("KAFKA kafka Kafka");
    OutputVerifier.compareKeyValue(readOutputRecord(), "kafka", 1L);
    OutputVerifier.compareKeyValue(readOutputRecord(), "kafka", 2L);
    OutputVerifier.compareKeyValue(readOutputRecord(), "kafka", 3L);
    assertEquals(readOutputRecord(), null);
  }

}
