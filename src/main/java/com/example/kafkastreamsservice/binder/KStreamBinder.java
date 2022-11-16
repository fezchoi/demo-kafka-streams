package com.example.kafkastreamsservice.binder;

import com.example.kafkastreamsservice.processor.KafkaHeaderExtractor;
import com.example.kafkastreamsservice.processor.ProcessLogger;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.function.Consumer;

@Configuration
@RequiredArgsConstructor
public class KStreamBinder {

    private final ProcessLogger processLogger;

    @Bean
    public Consumer<KStream<String, Map<String, Object>>> simpleConsumer() {
        return kstream1 -> {
            var kstream2 =
                    kstream1
                            .transformValues(KafkaHeaderExtractor::new)
                            .peek(processLogger.getValueInfoLogger("After Processor"));

            kstream1.peek(processLogger.getValueInfoLogger("Before Processor"));

            kstream1.peek(processLogger.getValueInfoLogger("Before Processor2"));

            kstream2.peek(processLogger.getValueInfoLogger("Before Processor3"));
        };
    }

    @Bean
    public Consumer<KStream<String, Map<String, Object>>> simpleConsumer2() {
        return kstream1 -> new Thread(() -> {
            var kstream2 =
                    kstream1
                            .transformValues(KafkaHeaderExtractor::new)
                            .peek(processLogger.getValueInfoLogger("After Processor"));

            kstream1.peek(processLogger.getValueInfoLogger("Before Processor"));

            kstream1.peek(processLogger.getValueInfoLogger("Before Processor2"));

            kstream2.peek(processLogger.getValueInfoLogger("Before Processor3"));
        });
    }
}
