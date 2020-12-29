package com.trendcore.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"streams-wordcount");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("streams-plaintext-input");


        KStream<String, String> words = source.flatMapValues((readOnlyKey, value) -> {
            return Arrays.asList(value.split(" "));
        });

        /*
            words.groupBy((key, value) -> value); here lambda function is keyselector
            hence when we return value from this function it will become
            key for next  function.
            In this case word will become key for next set of operations.
         */
        KGroupedStream<String, String> wordCount = words.groupBy((key, value) -> value);
        KTable<String, Long> count = wordCount.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

        count.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(),Serdes.Long()));


        Topology topology = builder.build();

        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology,properties);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
            @Override
            public void run() {
                kafkaStreams.close();
                countDownLatch.countDown();
            }
        });

        try {
            kafkaStreams.start();
            countDownLatch.await();
        } catch (InterruptedException e) {
            System.exit(1);
        }

    }

}
