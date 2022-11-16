package com.example.kafkastreamsservice.processor;

import com.example.kafkastreamsservice.common.Const;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ProcessLogger {

     private static final ObjectMapper mapper = new ObjectMapper();

    @Slf4j
    @RequiredArgsConstructor
    private static class ValueInfoLogger implements ForeachAction<String, Map<String, Object>> {

        private final String title;
        private final String[] content;

        @Override
        public void apply(String key, Map<String, Object> value) {
            String headerStr;
            String keyStr = (key != null ? key : "null");
            String valueStr;
            String contentStr = "| CONTENTS = ";

            try {
                Object headers = value.get(Const.HEADER);
                headerStr = mapper.writeValueAsString(headers);
            } catch (JsonProcessingException e) {
                headerStr = "null";
            }

            try {
                valueStr = mapper.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                valueStr = value.toString();
            }

            try {
                String str = String.join(", ", content);
                contentStr = (str.equals("")) ? "" : contentStr.concat(str);
            } catch (NullPointerException e) {
                contentStr = "";
            }

            log.info("[ProcessLogger] {} | HEADER = {} | KEY = {} | VALUE = {} {}", title, headerStr, keyStr, valueStr, contentStr);
        }
    }

    public ForeachAction<String, Map<String, Object>> getValueInfoLogger(final String title, final String... content) {
        return new ValueInfoLogger(title, content);
    }
}
