package com.example.kafkastreamsservice.binder;

import com.example.kafkastreamsservice.processor.KafkaHeaderExtractor;
import com.example.kafkastreamsservice.processor.ProcessLogger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
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
                            .peek(processLogger.getValueInfoLogger("1) Depth 2, Declare 1"));

            kstream1.peek(processLogger.getValueInfoLogger("1) Depth 1, Declare 2"));

            kstream1.peek(processLogger.getValueInfoLogger("1) Depth 1, Declare 3"));

            kstream2.peek(processLogger.getValueInfoLogger("1) Depth 3, Declare 4"));
        };
    }

//    private BlockingQueue<Consumer<KStream<String, Map<String, Object>>>> blockingQueue = new ArrayBlockingQueue<>(5);
    private final Sinks.Many<Message<Map<String, Object>>> multicast = Sinks.many().unicast().onBackpressureBuffer();

    @Bean
    public Consumer<Message<Map<String, Object>>> simpleConsumer2() {
        return message -> Mono
                .create(monoSink -> {
                    log.info("1 MonoSink  : key = " + message.getPayload().get("test-seq"));
                    final Sinks.EmitResult emitResult = multicast.tryEmitNext(message);
                    if (emitResult.isSuccess()) {
                        log.info("3 MonoSink  : key = " + message.getPayload().get("test-seq"));
                        monoSink.success();
                        log.info("4 MonoSink  : key = " + message.getPayload().get("test-seq"));
                    } else {
                        emitResult.orThrow();
                    }
                })
                .retryWhen(Retry.fixedDelay(5, Duration.ofMillis(500)))
                .subscribe();
    }

    @Bean
    public Supplier<Flux<Message<Map<String, Object>>>> generate() {
        return () -> multicast.asFlux().doOnNext(message -> log.info("2 generate  : key = " + message.getPayload().get("test-seq")));
    }

    @Bean
    public Function<Flux<Map<String, Object>>, Flux<Map<String, Object>>> process() {
        return flux -> flux
                .map(value -> {
                    log.info("5 process   : key = " + value.get("test-seq"));
                    return value;
                })
                .doOnNext(v -> log.info("6 process   : key = " + v.get("test-seq")));
    }

    @Bean
    public Consumer<Flux<Map<String, Object>>> subscribe() {
        return flux -> flux.subscribe(v -> log.info("7 subscribe : key = " + v.get("test-seq")));
    }

    private final BlockingQueue<KStream<String, Map<String, Object>>> blockingQueue = new ArrayBlockingQueue<>(30);
    private final MultiConsumer multiConsumer = new MultiConsumer(blockingQueue);

    @PostConstruct
    public void postConstruct() {
        new Thread(multiConsumer).start();
    }

    @Bean
    public Consumer<KStream<String, Map<String, Object>>> simpleConsumer3() {
        return kStream -> {
            kStream.peek(processLogger.getValueInfoLogger("MultiConsumer Consume "));
            try {
                log.info("BlockingQueue put"); // not printing
                blockingQueue.put(kStream);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @RequiredArgsConstructor
    private class MultiConsumer implements Runnable {

        private final BlockingQueue<KStream<String, Map<String, Object>>> blockingQueue;

        @Override
        public void run() {
            try {
                while (true) {
                    var kStream = blockingQueue.take();
                    log.info("BlockingQueue take - before"); // not printing
                    System.out.println("here"); // not printing
                    var kStream1 = kStream.peek(processLogger.getValueInfoLogger("MultiConsumer Thread 1"));
                    var kStream2 = kStream.peek(processLogger.getValueInfoLogger("MultiConsumer Thread 2"));
                    var kStream3 = kStream1.peek(processLogger.getValueInfoLogger("MultiConsumer Thread 3"));
                    log.info("BlockingQueue take - after"); // not printing
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Bean
    public Consumer<Message<Map<String, Object>>> simpleConsumer4() { // not working
        return message -> {
            log.info("Consumed");
            var kStream = getStreams(message);
            log.info(kStream.toString());
            var kStream1 = kStream.peek(processLogger.getValueInfoLogger("Process 1"));
            log.info(kStream1.toString());
            var kStream2 = kStream1.peek(processLogger.getValueInfoLogger("Process 2"));
            log.info(kStream2.toString());
            var kStream3 = kStream.peek(processLogger.getValueInfoLogger("Process 3"));
            log.info(kStream3.toString());
        };
    }

    private KStream<String, Map<String, Object>> getStreams(Message<Map<String, Object>> message) {
        KStream<String, Message<Map<String, Object>>> kStream = new StreamsBuilder().stream("");
        kStream.mapValues(value -> message);

        return kStream.transform(() -> new Transformer<>() {
            ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<String, Map<String, Object>> transform(String key, Message<Map<String, Object>> value) {
                value.getHeaders().forEach((k, v) -> context.headers().add(k, (byte[]) v));

                Map<String, Object> v = value.getPayload();

                return new KeyValue<>(null, v);
            }

            @Override
            public void close() {

            }
        });
    }
}
