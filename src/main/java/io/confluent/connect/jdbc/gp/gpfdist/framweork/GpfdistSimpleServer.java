/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.gp.gpfdist.framweork;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;


/**
 * Server implementation around reactor and netty providing endpoint
 * where data can be sent using a gpfdist protocol.
 *
 * @author Janne Valkealahti
 */
public class GpfdistSimpleServer {


    // make sigelton
    private static GpfdistSimpleServer instance = null;
    public static GpfdistSimpleServer getInstance() {
        if(instance == null) {
            instance = new GpfdistSimpleServer();
        }
        return instance;
    }

private GpfdistSimpleServer() {
        // do nothing
    }

    public void init(int port, boolean autoStop) {
     this.port = port;
        this.autoStop = autoStop;

    }



    private static final Logger log = LoggerFactory.getLogger(GpfdistServer.class);
    private  boolean autoStop;

    private List<List<String>> records;
    private int port;
    private HttpServer server;


    public void setRecords(List<List<String>> records) {
        this.records = records;
    }

    public List<List<String>> getRecords() {
        return records;
    }

    /**
     * Start a server.
     *
     * @return the http server
     * @throws Exception the exception
     */
    public synchronized HttpServer start() throws Exception {
        if (server == null) {
            server = startServer();
        }
        return server;
    }

    /**
     * Stop a server.
     *
     * @throws Exception the exception
     */
    public synchronized void stop() throws Exception {
        if (server != null) {
            int delay = 4000;
           server.stop(delay);
        }
        server = null;
    }

    /**
     * Gets the local port.
     *
     * @return the local port
     */
    public int getLocalPort() {
        return port;
    }

    private HttpServer startServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(getLocalPort()), 0);
        server.createContext("/data", new DataHandler());
        server.setExecutor(null); // Use the default executor
        server.start();
        log.info("Server is running on port "+getLocalPort());
        return server;
    }


     class DataHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            List<List<String>> batch = GpfdistSimpleServer.this.getBatch(); // Implement logic to get a batch of data
            String responseBody = convertDataToString(batch);

            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, responseBody.length());

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBody.getBytes());
            }
            if(autoStop){
                try {
                    stop();
                } catch (Exception e) {
                   log.error("Error stopping server", e);
                }
            }
        }
    }

    private List<List<String>> getBatch() {
        // Implement logic to get a batch of data
        return records;
    }

    private static String convertDataToString(List<List<String>> data) {
        // Implement logic to convert List<List<String>> to String
        // TODO use delimeters from config file instead
        StringBuilder sb = new StringBuilder();
        for (List<String> line : data) {
            sb.append(String.join(",", line)).append("\n");
        }
        return sb.toString();
    }


}
