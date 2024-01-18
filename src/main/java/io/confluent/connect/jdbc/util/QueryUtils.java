package io.confluent.connect.jdbc.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class QueryUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonConverter JSON_CONVERTER = new JsonConverter();
    private static final JsonConverter JSON_KEY_CONVERTER = new JsonConverter();
    private static final JsonConverter JSON_VALUE_CONVERTER = new JsonConverter();
    private static final JsonConverter JSON_HEADER_CONVERTER = new JsonConverter();
    private static final Pattern JSONPATH_PATTERN = Pattern.compile("\\$\\.[^\\s,)]*");

    static {
        Map<String, Object> configs = JsonConverterConfig.configDef().configKeys()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                            switch (e.getValue().name) {
                                case "converter.type":
                                    return "value";
                                case "schemas.enable":
                                    return "false";
                                default:
                                    return String.valueOf(e.getValue().defaultValue);
                            }
                        }));
        JSON_CONVERTER.configure(configs);
    }

    public static Map.Entry<String, Map<Integer, String>> parseQuery(final String query) {
        String newQuery = query;
        Matcher matcher = JSONPATH_PATTERN.matcher(query);
        Map<Integer, String> params = new HashMap<>();
        for (int i = 1; matcher.find(); i++) {
            String group = matcher.group(0);
            params.put(i, group);
            newQuery = newQuery.replace(group, "?");
        }
        return new AbstractMap.SimpleEntry<>(newQuery, params);
    }

    public static Map<String, Object> toMap(SinkRecord sinkRecord) {
        try {
            Map<String, Object> map = new HashMap<>();
            if (sinkRecord.key() != null) {
                map.put("key", OBJECT_MAPPER.readValue(JSON_CONVERTER.fromConnectData(null,
                        sinkRecord.keySchema(), sinkRecord.key()), Object.class));
            }
            if (sinkRecord.value() != null) {
                map.put("value", OBJECT_MAPPER.readValue(JSON_CONVERTER.fromConnectData(null,
                        sinkRecord.valueSchema(), sinkRecord.value()), Object.class));
            }
            Map<String, Object> headers = StreamSupport
                    .stream(sinkRecord.headers().spliterator(), false)
                    .map(header -> {
                        try {
                            Object o = OBJECT_MAPPER.readValue(JSON_CONVERTER
                                    .fromConnectHeader(null, header.key(),
                                            header.schema(), header.value()), Object.class);
                            return new AbstractMap.SimpleEntry<>(header.key(),o);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (!headers.isEmpty()) {
                map.put("headers", headers);
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJson(SinkRecord record) {
        try {
            return OBJECT_MAPPER.writeValueAsString(toMap(record));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
