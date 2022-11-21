package com.example.kafkastreamsservice.common.kafka;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Getter
@Setter
@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private final String topic = "simple.topic.test";
    private final String topic2 = "simple.topic.test2";
    private final String topic3 = "simple.topic.test3";
    private final String topic4 = "simple.topic.test4";
    private final String group = "kafka-streams-service-test";

    private String payload = null;
    private String value = null;
    private CountDownLatch latch = new CountDownLatch(20);

    @KafkaListener(topics = topic, groupId = group)
    public void receive(ConsumerRecord<?, ?> consumerRecord) {
        String key = new String((byte[]) consumerRecord.key());
        String value = new String((byte[]) consumerRecord.value());
        log.info("[KafkaConsumer] receive | topic : {}, partition : {}, key : {}, value : {}", consumerRecord.topic(), consumerRecord.partition(), key, value);
        setPayload(consumerRecord.toString());
        setValue(value);
        latch.countDown();
    }

    @KafkaListener(topics = topic2, groupId = group)
    public void receive2(ConsumerRecord<?, ?> consumerRecord) {
        String key = new String((byte[]) consumerRecord.key());
        String value = new String((byte[]) consumerRecord.value());
        log.info("[KafkaConsumer] receive | topic : {}, partition : {}, key : {}, value : {}", consumerRecord.topic(), consumerRecord.partition(), key, value);
        setPayload(consumerRecord.toString());
        setValue(value);
        latch.countDown();
    }

    @KafkaListener(topics = topic3, groupId = group)
    public void receive3(ConsumerRecord<?, ?> consumerRecord) {
        String key = new String((byte[]) consumerRecord.key());
        String value = new String((byte[]) consumerRecord.value());
        log.info("[KafkaConsumer] receive | topic : {}, partition : {}, key : {}, value : {}", consumerRecord.topic(), consumerRecord.partition(), key, value);
        setPayload(consumerRecord.toString());
        setValue(value);
        latch.countDown();
    }

    @KafkaListener(topics = topic4, groupId = group)
    public void receive4(ConsumerRecord<?, ?> consumerRecord) {
        String key = new String((byte[]) consumerRecord.key());
        String value = new String((byte[]) consumerRecord.value());
        log.info("[KafkaConsumer] receive | topic : {}, partition : {}, key : {}, value : {}", consumerRecord.topic(), consumerRecord.partition(), key, value);
        setPayload(consumerRecord.toString());
        setValue(value);
        latch.countDown();
    }
}
