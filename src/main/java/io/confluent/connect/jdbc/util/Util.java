package io.confluent.connect.jdbc.util;

public class Util {
    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }
}
