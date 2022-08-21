package com.github.gustavoflor.usereventenricher;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class UserEventEnricherProducer {
    private static final Properties PROPERTIES = new Properties();
    private static final KafkaProducer<String, String> KAFKA_PRODUCER;
    private static final String USER_PURCHASES_TOPIC = "user-purchases";
    private static final String USER_TABLE_TOPIC = "user-table";

    static {
        PROPERTIES.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        PROPERTIES.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PROPERTIES.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PROPERTIES.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        PROPERTIES.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        PROPERTIES.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        PROPERTIES.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        KAFKA_PRODUCER = new KafkaProducer<>(PROPERTIES);
    }

    public static void main(String[] args) {
        try {
            KAFKA_PRODUCER.send(userRecord("gustavo", "{email=(gustavo@mock.com),age=(19)}")).get();
            KAFKA_PRODUCER.send(purchaseRecord("gustavo", "Java Course [1]")).get();
            Thread.sleep(10000);

            KAFKA_PRODUCER.send(purchaseRecord("stephan", "Kafka Book [2]")).get();
            Thread.sleep(10000);

            KAFKA_PRODUCER.send(userRecord("gustavo", "{email=(gusta@mail.com),age=(24)}")).get();
            KAFKA_PRODUCER.send(purchaseRecord("gustavo", "Macbook - Apple M1 [3]")).get();
            Thread.sleep(10000);

            KAFKA_PRODUCER.send(purchaseRecord("lillian", "iPhone X [4]")).get();
            KAFKA_PRODUCER.send(userRecord("lillian", "{email=(li@mail.com),age=(20)}")).get();
            KAFKA_PRODUCER.send(purchaseRecord("lillian", "Macbook - Intel [4]")).get();
            KAFKA_PRODUCER.send(userRecord("lillian", null)).get();
            Thread.sleep(10000);

            KAFKA_PRODUCER.send(userRecord("ana", "{email=(ana@mock.com),age=(23)}")).get();
            KAFKA_PRODUCER.send(userRecord("ana", null)).get();
            KAFKA_PRODUCER.send(purchaseRecord("ana", "React Native Bootcamp [5]")).get();
            Thread.sleep(10000);
        } catch (InterruptedException | ExecutionException e) {
            // DO NOTHING
        }
        KAFKA_PRODUCER.close();
    }

    private static ProducerRecord<String, String> userRecord(final String username, final String details) {
        return new ProducerRecord<>(USER_TABLE_TOPIC, username, details);
    }

    private static ProducerRecord<String, String> purchaseRecord(final String username, final String details) {
        return new ProducerRecord<>(USER_PURCHASES_TOPIC, username, details);
    }
}
