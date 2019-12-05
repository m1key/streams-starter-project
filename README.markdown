# Streams Starter App

Following https://www.udemy.com/course/kafka-streams/

## Colours Exercise

`cleanup.policy=compact` is not required.

```
zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
kafka-server-start.sh -daemon /opt/kafka/config/server.properties
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 \
    --topic fav-colours-input
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 \
    --topic fav-colours-intermediary --config cleanup.policy=compact
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 \
    --topic fav-colours-output --config cleanup.policy=compact

kafka-console-producer.sh --broker-list localhost:9092 --topic fav-colours-input

kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic fav-colours-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

Data:
```
stephane,blue
john,green
stephane,red
alice,red
invalid_row
```