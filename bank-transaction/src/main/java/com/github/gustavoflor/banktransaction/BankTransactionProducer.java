package com.github.gustavoflor.banktransaction;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class BankTransactionProducer {
    private static final Properties PROPERTIES = new Properties();
    private static final KafkaProducer<String, String> KAFKA_PRODUCER;
    private static final JsonNodeFactory JSON_NODE_FACTORY = new JsonNodeFactory(false);
    private static final Faker FAKER = new Faker();
    private static final String TOPIC = "bank-transactions";

    static {
        PROPERTIES.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        PROPERTIES.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PROPERTIES.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PROPERTIES.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        PROPERTIES.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        PROPERTIES.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
        PROPERTIES.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        KAFKA_PRODUCER = new KafkaProducer<>(PROPERTIES);
    }

    public static void main(String[] args) {
        while (true) {
            try {
                createAndSendTransaction("stephane");
                createAndSendTransaction("lillian");
                createAndSendTransaction("gustavo");
            } catch (InterruptedException e) {
                break;
            }
        }
        KAFKA_PRODUCER.close();
    }

    private static void createAndSendTransaction(final String customerName) throws InterruptedException {
        final var amount = FAKER.number().randomDouble(2, 10L, 50L);
        final var occurredAt = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now());
        final var transaction = JSON_NODE_FACTORY.objectNode()
                .put("name", customerName)
                .put("amount", amount)
                .put("occurredAt", occurredAt);
        KAFKA_PRODUCER.send(new ProducerRecord<>(TOPIC, customerName, transaction.toString()));
        Thread.sleep(100);
    }
}
