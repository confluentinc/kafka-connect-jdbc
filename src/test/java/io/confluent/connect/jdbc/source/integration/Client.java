package io.confluent.connect.jdbc.source.integration;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;

//public class Client implements AutoCloseable {
//
//    private CloseableHttpClient httpClient;
//
//    public Client() {
//        httpClient = HttpClients.createDefault();
//    }
//
//    public CloseableHttpClient getHttpClient() {
//        return httpClient;
//    }
//
//    @Override
//    public void close() throws IOException {
//        httpClient.close();
//    }
//
//
//}
