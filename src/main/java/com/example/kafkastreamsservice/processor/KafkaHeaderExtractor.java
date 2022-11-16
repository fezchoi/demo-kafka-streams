package com.example.kafkastreamsservice.processor;

import com.example.kafkastreamsservice.common.Const;
import com.example.kafkastreamsservice.util.KafkaUtil;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.HashMap;
import java.util.Map;

public class KafkaHeaderExtractor implements ValueTransformer<Map<String, Object>, Map<String, Object>> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public Map<String, Object> transform(Map<String, Object> value) {
        var newValue = new HashMap<>(value);
        newValue.put(Const.HEADER, KafkaUtil.readHeaderMapAll(context.headers()));
        newValue.put(Const.TOPIC, context.topic());

//        try {
//            Thread.sleep(5000);
//            System.out.println("thread sleep for 5 sec");
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

        return newValue;
    }

    @Override
    public void close() {

    }
}
