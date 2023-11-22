package io.confluent.connect.jdbc.gp.gpfdist.framweork;

public class Message<T> {
    private T payload;
    // constructor
    Message(T payload) {
        this.payload = payload;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }
}
