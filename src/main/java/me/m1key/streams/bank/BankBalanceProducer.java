package me.m1key.streams.bank;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class BankBalanceProducer {

    private static final Logger log = LoggerFactory.getLogger(BankBalanceProducer.class);
    private static final Random RANDOM = new Random();
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss");

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "bank-balance-producer");
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        for (int i = 0; i < 10; i++) {
            String key = UUID.randomUUID().toString();
            final ProducerRecord<String, String> record =
                    new ProducerRecord<>("bank-balance-input", key, newRecordJson());
            producer.send(record, (metadata, e) -> {
                if (e != null) {
                    log.warn("Send failed for record {}", record, e);
                } else {
                    log.info("Send succeeded for record {}", record);
                }
            });
        }

        producer.close();
    }

    private static String newRecordJson() {
        String name = randomName();
        int amount = randomAmount();
        String date = FORMATTER.format(LocalDateTime.now());

        return String.format("{\"Name\":\"%s\", \"amount\":%d, \"time\":\"%s\"}", name, amount, date);
    }

    private static String randomName() {
        String[] names = {"John", "Alice", "Bob", "Mary", "Sue", "George", "Francois", "Moe"};
        return names[RANDOM.nextInt(names.length)];
    }

    private static int randomAmount() {
        return RANDOM.nextInt(1000);
    }
}
