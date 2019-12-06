package me.m1key.streams.bank;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BankBalanceStream {

    private static Pattern NAME_PATTERN = Pattern.compile(".*Name\":\"(.+?)\".*");
    private static Pattern AMOUNT_PATTERN = Pattern.compile(".*amount\":([0-9]+).*");

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);


        StreamsBuilder builder = new StreamsBuilder();
        // 1. Stream from Kafka.
        KStream<String, String> bankBalanceInput = builder.stream("bank-balance-input",
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Integer> userAmount = bankBalanceInput
                .selectKey((originalKey, value) -> extractName(value))
                .mapValues(BankBalanceStream::extractAmount);

        KTable<String, Integer> aggregated = userAmount
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .aggregate(
                        () -> 0, // Initialise,
                        (aggregateKey, currentValue, aggregateValue) -> currentValue + aggregateValue,
                        Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                                .withValueSerde(Serdes.Integer())
                        .withKeySerde(Serdes.String()));
        aggregated.toStream().to("bank-balance-aggregated");


        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

    }

    static int extractAmount(String value) {
        // TODO use a JSON library.
        Matcher matcher = AMOUNT_PATTERN.matcher(value);
        if (matcher.matches()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            throw new RuntimeException(); //TODO optional
        }
    }

    static String extractName(String value) {
        // TODO use a JSON library.
        Matcher matcher = NAME_PATTERN.matcher(value);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new RuntimeException(); //TODO optional
        }
    }
}
