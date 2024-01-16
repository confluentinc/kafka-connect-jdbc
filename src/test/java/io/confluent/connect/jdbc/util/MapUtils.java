package io.confluent.connect.jdbc.util;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface MapUtils {

    static <T> Map<String, T> of(Object... values) {
        if (values.length % 2 != 0) throw new IllegalArgumentException("values should be even");
        return IntStream.range(0, values.length)
                .filter(i -> (i + 1) % 2 != 0)
                .mapToObj(i -> Pair.of((String) values[i], (T) values[i + 1]))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

}
