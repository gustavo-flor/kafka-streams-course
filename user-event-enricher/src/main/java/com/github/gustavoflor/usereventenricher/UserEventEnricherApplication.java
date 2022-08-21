package com.github.gustavoflor.usereventenricher;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.UUID;

import static java.lang.String.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public class UserEventEnricherApplication {
    private static final Properties PROPERTIES;
    private static final String INPUT_TOPIC = "user-purchases";
    private static final String GLOBAL_TABLE_TOPIC = "user-table";
    private static final String USER_PURCHASES_ENRICHED_TOPIC_JOIN = "user-purchases-enriched-join";
    private static final String USER_PURCHASES_ENRICHED_TOPIC_LEFT_JOIN = "user-purchases-enriched-left-join";

    static {
        PROPERTIES = new Properties();
        PROPERTIES.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        PROPERTIES.setProperty(APPLICATION_ID_CONFIG, "user-event-enricher-app");
        PROPERTIES.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        PROPERTIES.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        PROPERTIES.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        PROPERTIES.setProperty(STATE_DIR_CONFIG, format("/tmp/kafka-streams/%s/%s", PROPERTIES.get(APPLICATION_ID_CONFIG), UUID.randomUUID()));
    }

    public static void main(String[] args) {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final GlobalKTable<String, String> usersGlobalTable = streamsBuilder.globalTable(GLOBAL_TABLE_TOPIC);
        final KStream<String, String> userPurchases = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        userPurchases.join(usersGlobalTable, (key, value) -> key, UserEventEnricherApplication::enrichUserPurchase)
                .to(USER_PURCHASES_ENRICHED_TOPIC_JOIN);
        userPurchases.leftJoin(usersGlobalTable, (key, value) -> key, UserEventEnricherApplication::enrichUserPurchase)
                .to(USER_PURCHASES_ENRICHED_TOPIC_LEFT_JOIN);
        startStreaming(streamsBuilder);
    }

    private static String enrichUserPurchase(final String purchase, final String user) {
        if (user != null) {
            return format("{purchase=(%s),user=(%s)}", purchase, user);
        }
        return format("{purchase=(%s),user=NULL}", purchase);
    }

    private static void startStreaming(final StreamsBuilder streamsBuilder) {
        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), PROPERTIES);
        kafkaStreams.cleanUp(); // ONLY FOR DEVELOPMENT
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
