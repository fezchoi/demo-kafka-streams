package com.example.kafkastreamsservice.common.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaProducer {

    private final KafkaTemplate<String, Map<String, Object>> kafkaTemplate;

    public void send(String topic, String key, Map<String, Object> value) {
        kafkaTemplate.send(topic, key, value);

        log.info("[KafkaProducer] send    | topic : {}, key : {}, value : {}", topic, key, value.toString());
    }

    public void send(ProducerRecord<String, Map<String, Object>> record) {
        kafkaTemplate.send(record);

        var headerList = Arrays
                .stream(record.headers().toArray())
                .map(header -> Map.of(header.key(), new String(header.value())))
                .toList();
        log.info("[KafkaProducer] send    | topic : {}, partition : {}, headers : {}, key : {}, value : {}", record.topic(), record.partition(), headerList, record.key(), record.value());
    }
}
