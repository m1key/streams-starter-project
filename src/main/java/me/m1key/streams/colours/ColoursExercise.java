package me.m1key.streams.colours;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Check the README.markdown file for command line instructions.
 */
public class ColoursExercise {

    private static Pattern NAME_COLOUR = Pattern.compile("(.+),(.+)");

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fav-colours");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // 1. Stream from Kafka.
        KStream<String,String> favouriteColoursInput = builder.stream("fav-colours-input");

        // Print all valid values:
//        favouriteColoursInput.filter(isValidFavouriteColour()).print(Printed.toSysOut());

        favouriteColoursInput
                // Only select values that meet our criteria.
                // Sample value: (null, Dzinman,red)
                .filter(isValidFavouriteColour())
                // The key is null at the moment. Set the key to be equal to the name.
                // Sample value: (Dzinman, Dzinman,red)
                .selectKey((originalKey, value) -> extractName(value))
                // Extract the colour and set it as value.
                // Sample value: (Dzinman, red)
                .mapValues(ColoursExercise::extractColour)
                .to("fav-colours-intermediary");

        KTable<String, String> table = builder.table("fav-colours-intermediary");
        // Group by value. This is the step where duplicates are actually going to be removed.
        KTable<String, Long> counts = table.groupBy((name, colour) -> new KeyValue<>(colour, colour)).count();

        // Write to output.
        counts.toStream().to("fav-colours-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        // In development only:
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String extractName(String value) {
        return extractGroup(value, 1);
    }

    private static String extractColour(String value) {
        return extractGroup(value, 2);
    }

    private static String extractGroup(String value, int groupNumber) {
        Matcher matcher = NAME_COLOUR.matcher(value);
        if (matcher.matches()) {
            return matcher.group(groupNumber);
        } else {
            throw new RuntimeException(); //TODO optional
        }
    }

    private static Predicate<String, String> isValidFavouriteColour() {
        return (key, value) -> NAME_COLOUR.matcher(value).matches();
    }
}
