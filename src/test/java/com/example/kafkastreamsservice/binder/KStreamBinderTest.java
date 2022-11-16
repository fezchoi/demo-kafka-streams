package com.example.kafkastreamsservice.binder;

import com.example.kafkastreamsservice.common.kafka.KafkaConsumer;
import com.example.kafkastreamsservice.common.kafka.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        topics = {"simple.topic.test", "simple.topic.test2"},
        partitions = 5,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"}
)
class KStreamBinderTest {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private KafkaConsumer kafkaConsumer;

//    @Autowired
//    private Consumer simpleConsumer;

//    @Test
//    void embeddedKafkaTest() throws InterruptedException {
//        //given
//        String topic = "simple.topic.test";
//        String key = null;
//        Map<String, Object> value = Map.of("test-key", "test-value");
//
//        //when
//        kafkaProducer.send(topic, key, value);
//        kafkaConsumer.getLatch().await(10000, TimeUnit.MILLISECONDS);
//
//        //then
//        assertThat(kafkaConsumer.getLatch().getCount(), Matchers.equalTo(0L));
//        assertThat(kafkaConsumer.getValue(), Matchers.containsString("test-value"));
//    }

    @Test
    void simpleConsumerKafkaTest() throws InterruptedException {
        //given
        String topic = "simple.topic.test";
        String topic2 = "simple.topic.test2";
        String key = null;
        Map<String, Object> value = Map.of("test-key", "test-value");

        //when
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, Map<String, Object>> record2 = new ProducerRecord<>(topic2, String.valueOf(i + 10), value);
            record2.headers().add("test-header-key", "test-header-value".getBytes(StandardCharsets.UTF_8));
            kafkaProducer.send(record2);
            ProducerRecord<String, Map<String, Object>> record = new ProducerRecord<>(topic, String.valueOf(i), value);
            record.headers().add("test-header-key", "test-header-value".getBytes(StandardCharsets.UTF_8));
            kafkaProducer.send(record);
        }
        kafkaConsumer.getLatch().await(60000, TimeUnit.MILLISECONDS);

        //then
        assertThat(kafkaConsumer.getLatch().getCount(), Matchers.equalTo(0L));
        assertThat(kafkaConsumer.getValue(), Matchers.containsString("test-value"));
    }

//    @Test
//    void simpleConsumerKafkaTest2() throws InterruptedException {
//        //given
//        String topic = "simple.topic.test2";
//        String key = null;
//        Map<String, Object> value = Map.of("test-key", "test-value");
//
//        //when
//        for (int i = 0; i < 10; i++) {
//            ProducerRecord<String, Map<String, Object>> record = new ProducerRecord<>(topic, String.valueOf(i + 10), value);
//            record.headers().add("test-header-key", "test-header-value".getBytes(StandardCharsets.UTF_8));
//            kafkaProducer.send(record);
//        }
//        kafkaConsumer.getLatch().await(10000, TimeUnit.MILLISECONDS);
//
//        //then
////        assertThat(kafkaConsumer.getLatch().getCount(), Matchers.equalTo(0L));
////        assertThat(kafkaConsumer.getValue(), Matchers.containsString("test-value"));
//    }


//    @Test
//    void simpleConsumerKStreamTest() throws InterruptedException {
//        //given
//        String topic = "simple.topic";
//
//        Properties props = new Properties();
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,  "localhost:9092");
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-service-test");
//
//        StreamsConfig streamsConfig = new StreamsConfig(props);
//        Serde<String> stringSerde = new Serdes.StringSerde();
//        Serde<Map<String, Object>> jsonSerde = new JsonSerde<>();
//        StreamsBuilder builder = new StreamsBuilder();
//
//        KStream<String, Map<String, Object>> inputKStream = builder.stream("test", Consumed.with(stringSerde, jsonSerde))
//                .transformValues(() -> new ValueTransformer<>() {
//                    ProcessorContext context;
//
//                    @Override
//                    public void init(ProcessorContext context) {
//                        this.context = context;
//                    }
//
//                    @Override
//                    public Map<String, Object> transform(Map<String, Object> value) {
//                        context.headers().add("test-header-key", "test-header-value".getBytes(StandardCharsets.UTF_8));
//                        return Map.of("test-key", "test-value");
//                    }
//
//                    @Override
//                    public void close() {
//
//                    }
//                });
//
//        inputKStream.to(topic);
//
//        //when
////        simpleConsumer.accept(inputKStream);
//        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
//        kafkaStreams.start();
//        Thread.sleep(10000);
//        kafkaStreams.close();
//
//        //then
//    }
}