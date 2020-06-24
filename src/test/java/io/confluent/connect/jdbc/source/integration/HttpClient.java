package io.confluent.connect.jdbc.source.integration;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

public class HttpClient implements AutoCloseable {

    private CloseableHttpClient httpClient;

    public HttpClient() {
        httpClient = HttpClients.createDefault();
    }

    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }


}
