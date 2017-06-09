package io.confluent.connect.jdbc.source;

/**
 * Created by shawnvarghese on 6/9/17.
 */
public interface Transform {

    public String transformString(String value, String transformer);
}
