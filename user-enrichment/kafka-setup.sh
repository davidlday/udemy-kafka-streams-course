#!/bin/bash

# create input topic for user purchases
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic purchases

# create table of user information - log compacted for optimisation
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic users-table --config cleanup.policy=compact

# create out topic for user purchases enriched with user data (left join)
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases-left-join

# create out topic for user purchases enriched with user data (inner join)
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases-join

# start a consumer on the output topic (left join)
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic user-purchases-left-join \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer


# start a consumer on the output topic (inner join)
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic user-purchases-join \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
