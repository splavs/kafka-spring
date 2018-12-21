package com.splavs.kafkaspring.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class TextProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private Random random = new Random();

    @Autowired
    public TextProducer(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 10000)
    public void produceMessage() {
        kafkaTemplate.send(new ProducerRecord<>("test", null, generateString()));
    }

    private String generateString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < random.nextInt(50); i++) {
            stringBuilder.append(generateWord()).append(" ");
        }
        return stringBuilder.toString();
    }

    private String generateWord() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < random.nextInt(20); i++) {
            int generatedChar = 'a' + random.nextInt(25);
            stringBuilder.append((char) generatedChar);
        }
        return stringBuilder.toString();
    }
}
