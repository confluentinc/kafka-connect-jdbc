package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

public class StringUtils {
    public static boolean hasText(String string) {
        return string != null && string.length() > 0;
    }

    public static char[] arrayToCommaDelimitedString(String[] array) {
        return String.join(",", array).toCharArray();
    }
}
