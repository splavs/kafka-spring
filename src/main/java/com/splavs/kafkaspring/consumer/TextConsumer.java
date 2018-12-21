package com.splavs.kafkaspring.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class TextConsumer {

    @KafkaListener(topics = "streams-wordcount-output")
    public void processMessage(@Payload ConsumerRecord wordCountRecord) {
        System.out.println(wordCountRecord);

    }
}
