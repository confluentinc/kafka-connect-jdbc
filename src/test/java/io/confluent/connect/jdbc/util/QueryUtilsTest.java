package io.confluent.connect.jdbc.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class QueryUtilsTest {

    @Test
    public void test() {
        Map.Entry<String, Map<Integer, String>> actual = QueryUtils
                .parseQuery("SELECT * FROM table where field_a = $.key.a AND field_b = $.value.b");
        Assert.assertEquals("SELECT * FROM table where field_a = ? AND field_b = ?", actual.getKey());
        Assert.assertEquals(MapUtils.<Integer, String>of(1, "$.key.a", 2, "$.value.b"), actual.getValue());
    }
}
