package com.github.gustavoflor.kafkastreamscourse.favouritecolor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class FavouriteColorApplication {
    private static final Properties PROPERTIES;
    private static final String INPUT_TOPIC = "favourite-color-input";
    private static final String INTERMEDIARY_TOPIC = "favourite-color-stream";
    private static final String OUTPUT_TOPIC = "favourite-color-output";
    private static final String COMMA = ",";
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\w+,\\w+");
    private static final List<String> AVAILABLE_COLORS = List.of("red", "green", "blue");

    static {
        PROPERTIES = new Properties();
        PROPERTIES.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        PROPERTIES.setProperty(APPLICATION_ID_CONFIG, "favourite-color-application-1");
        PROPERTIES.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        PROPERTIES.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        PROPERTIES.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        PROPERTIES.setProperty(STATE_DIR_CONFIG, String.format("/tmp/kafka-streams/%s/%s", PROPERTIES.get(APPLICATION_ID_CONFIG), UUID.randomUUID()));
    }

    public static void main(String[] args) {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.stream(INPUT_TOPIC)
                .mapValues(Object::toString)
                .filter((key, value) -> TEMPLATE_PATTERN.matcher(value).matches())
                .selectKey((key, value) -> value.split(COMMA)[0])
                .mapValues(value -> value.split(COMMA)[1].toLowerCase())
                .filter((key, value) -> AVAILABLE_COLORS.stream().anyMatch(color -> color.equals(value)))
                .to(INTERMEDIARY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        streamsBuilder.table(INTERMEDIARY_TOPIC)
                .groupBy((key, value) -> new KeyValue<>(value.toString(), key))
                .count(Named.as("FavouriteColorCounter"))
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        startStreaming(streamsBuilder);
    }

    private static void startStreaming(final StreamsBuilder streamsBuilder) {
        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), PROPERTIES);
        kafkaStreams.cleanUp(); // ONLY FOR DEVELOPMENT
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
