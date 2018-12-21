package com.splavs.kafkaspring.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Locale;

@Component
public class WordCount {
    @Bean
    public KStream<String, Long> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, Long> stream = streamsBuilder.<String, String>stream("test", Consumed.with(Serdes.String(), Serdes.String()))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
                .toStream();

        stream.to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        stream.print(Printed.toSysOut());

        return stream;
    }

}
