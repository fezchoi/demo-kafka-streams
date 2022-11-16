package com.example.kafkastreamsservice.util;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.*;

public class KafkaUtil {

    public static String readHeader(final Headers headers, final String key) {
        Header header = headers.lastHeader(key);
        return header != null ? new String(header.value()) : "null";
    }

    public static Map<String, String> readHeaderMap(final Headers headers, final String[] keys) {
        Map<String, String> headerMap = new HashMap<>();

        Arrays.stream(keys).forEach(key -> {
                Header header = headers.lastHeader(key);
                headerMap.put(key, header != null ? new String(header.value()) : "null");
            }
        );

        return headerMap;
    }

    public static Map<String, String> readHeaderMapAll(final Headers headers) {
        Map<String, String> headerMap = new HashMap<>();

        headers.forEach(header -> {
            headerMap.put(header.key(), new String(header.value()));
        });

        return headerMap;
    }
}
