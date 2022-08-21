package com.github.gustavoflor.bankbalance;

import com.github.gustavoflor.bankbalance.json.JsonDeserializer;
import com.github.gustavoflor.bankbalance.json.JsonSerializer;
import com.github.gustavoflor.bankbalance.model.Balance;
import com.github.gustavoflor.bankbalance.model.Transaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class BankBalanceApplication {
    private static final Properties PROPERTIES;
    private static final String INPUT_TOPIC = "bank-transactions";
    private static final String OUTPUT_TOPIC = "bank-balances";
    private static final Logger LOGGER = LoggerFactory.getLogger(BankBalanceApplication.class);

    static {
        PROPERTIES = new Properties();
        PROPERTIES.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        PROPERTIES.setProperty(APPLICATION_ID_CONFIG, "bank-balance-app");
        PROPERTIES.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        PROPERTIES.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        PROPERTIES.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        PROPERTIES.setProperty(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        PROPERTIES.setProperty(REPLICATION_FACTOR_CONFIG, "1");
        PROPERTIES.setProperty(STATE_DIR_CONFIG, String.format("/tmp/kafka-streams/%s/%s", PROPERTIES.get(APPLICATION_ID_CONFIG), UUID.randomUUID()));
    }

    public static void main(String[] args) {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), getTransactionSerde()))
                .peek((key, value) -> LOGGER.info("transaction received = {} [{}]", value, key))
                .groupByKey()
                .aggregate(Balance::new, (name, transaction, balance) -> {
                    balance.setName(name);
                    balance.addTotalMoney(transaction.getAmount());
                    balance.setLatestTime(balance.getLatestTime());
                    return balance;
                }, Named.as("bank-balance-agg"), Materialized.with(Serdes.String(), getBalanceSerde()))
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), getBalanceSerde()));
        startStreaming(streamsBuilder);
    }

    private static void startStreaming(final StreamsBuilder streamsBuilder) {
        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), PROPERTIES);
        kafkaStreams.cleanUp(); // ONLY FOR DEVELOPMENT
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static Serde<Balance> getBalanceSerde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Balance.class));
    }

    private static Serde<Transaction> getTransactionSerde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Transaction.class));
    }
}
