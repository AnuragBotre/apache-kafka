package com.trendcore.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountDemo {

    public static final String INPUT_TOPIC = "streams-plaintext-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-output";

    static Properties getStreamsConfig() {
        final Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    public static void main(String[] args) {
        /*
            How to work this application
            http://kafka.apache.org/25/documentation/streams/quickstart
            1. start zoo keeper - if not started
            2. start kafka server - if not started
            3. create input topic - streams-plaintext-input
                bin/kafka-topics.sh --create \
                    --bootstrap-server localhost:9092 \
                    --replication-factor 1 \
                    --partitions 1 \
                    --topic streams-plaintext-input
            4. create input topic - streams-wordcount-output
                bin/kafka-topics.sh --create \
                    --bootstrap-server localhost:9092 \
                    --replication-factor 1 \
                    --partitions 1 \
                    --topic streams-wordcount-output \
                    --config cleanup.policy=compact
            5. then run this Main program
            6. Then run command line kafka producer -
                    bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic streams-plaintext-input
                    and start entering input
            7. Then Run command line kafka consumer -
                bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
                --topic streams-wordcount-output \
                --from-beginning \
                --formatter kafka.tools.DefaultMessageFormatter \
                --property print.key=true \
                --property print.value=true \
                --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
                --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

                observe the output in the kafka-console-consumer.
         */
        final Properties props = getStreamsConfig();

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        createWordCountStreamBuilder(streamsBuilder);

        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            System.exit(1);
        }
    }

    private static void createWordCountStreamBuilder(StreamsBuilder streamsBuilder) {
        KStream<String, String> source = streamsBuilder.stream(INPUT_TOPIC);

        KTable<String, Long> count = source
                .flatMapValues((readOnlyKey, value) -> Arrays.asList(value.split(" ")))
                .groupBy((key, value) -> value)
                .count();

        count.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

}
