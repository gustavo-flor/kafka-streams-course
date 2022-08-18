package com.github.gustavoflor.kafkastreamscourse.wordcount;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public class WordCountApplication {
    private static final Properties PROPERTIES;
    private static final String INPUT_TOPIC = "word-count-input";
    private static final String OUTPUT_TOPIC = "word-count-output";
    private static final String COUNTER_NAME = "WordCountCounter";
    private static final String SPACE = " ";

    static {
        PROPERTIES = new Properties();
        PROPERTIES.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        PROPERTIES.setProperty(APPLICATION_ID_CONFIG, "word-count-application-1");
        PROPERTIES.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        PROPERTIES.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        PROPERTIES.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        PROPERTIES.setProperty(STATE_DIR_CONFIG, String.format("/tmp/kafka-streams/%s/%s", PROPERTIES.get(APPLICATION_ID_CONFIG), UUID.randomUUID()));
    }

    public static void main(String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> wordCountStream = builder.stream(INPUT_TOPIC);
        final KTable<String, Long> wordCountTable = getWordCountTable(wordCountStream);
        wordCountTable.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        final KafkaStreams streams = new KafkaStreams(builder.build(), PROPERTIES);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KTable<String, Long> getWordCountTable(final KStream<String, String> wordCountStream) {
        return wordCountStream.mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(SPACE)))
                .selectKey((key, value) -> value)
                .groupByKey()
                .count(Named.as(COUNTER_NAME));
    }
}
