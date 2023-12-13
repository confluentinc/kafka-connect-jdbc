//package io.confluent.connect.jdbc.gp.gpfdist;
//
//import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
//import io.vertx.core.Vertx;
//import io.vertx.core.http.HttpServer;
//import io.vertx.core.http.HttpServerRequest;
//import io.vertx.core.http.HttpServerResponse;
//import io.vertx.core.json.JsonObject;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class VertXHttpServer {
//
//    private static final Log log = LogFactory.getLog(VertXHttpServer.class);
//
//    private static final VertXHttpServer INSTANCE = new VertXHttpServer();
//
//    private final Vertx vertx;
//    private final HttpServer httpServer;
//    private final Map<String, List<String>> pathDataMap;
//    private JdbcSinkConfig config;
//    private boolean initialized;
//
//    private VertXHttpServer() {
//        this.vertx = Vertx.vertx();
//        this.httpServer = vertx.createHttpServer();
//        this.pathDataMap = new ConcurrentHashMap<>();
//
//        // Set up request handler
//        httpServer.requestHandler(this::handleRequest);
//    }
//
//    public static VertXHttpServer getInstance() {
//        return INSTANCE;
//    }
//
//
//    public void start() {
//        int port = config.getGpfdistPort();
//        if (isServerRunning(port)) {
//            throw new IllegalStateException("Server is already running on port " + port);
//        }
//
//        httpServer.listen(port, result -> {
//            if (result.succeeded()) {
//                log.info("Server started on port " + port);
//            } else {
//                log.error("Failed to start the server: " + result.cause());
//            }
//        });
//    }
//
//    public void addData(String path, List<String> newData) {
//        pathDataMap.computeIfAbsent(path, k -> new ArrayList<>()).addAll(newData);
//
//    }
//
//    public JsonObject getStatus() {
//        return new JsonObject().put("status", "Server is running").put("data", pathDataMap);
//    }
//
//    private void handleRequest(HttpServerRequest request) {
//        String path = request.path();
//        if ("/status".equals(path)) {
//            JsonObject status = getStatus();
//            request.response().end(status.encode());
//        } else{
//
//            List<String> data = pathDataMap.getOrDefault(path, new ArrayList<>());
//            if(data.isEmpty()){
//                log.error("No data found for path: " + path);
//                request.response().setStatusCode(404).end("Not Found");
//                return;
//            }
//
//            HttpServerResponse response = request.response();
//
//            response.putHeader("Content-type", "text/plain");
//            response.putHeader("Expires", "0");
//            response.putHeader("X-GPFDIST-VERSION", "1.0.0");
//            response.putHeader("X-GP-PROTO", "1");
//            response.putHeader("Cache-Control", "no-cache");
//            response.putHeader("Connection", "close");
//
//            // Use a Vert.x Pump to send a stream of strings to the response
//            // In this example, we send "Hello", "Vert.x", and "World" as a stream of strings
////            response.setChunked(true);
////            response.write("Hello\n");
////            response.write("Vert.x\n");
////            response.write("World");
//            response.write(String.join(",", data));
//
//            // End the response
//            response.end();
//
////            vertx.executeBlocking(future -> {
////                // Simulate processing time or data retrieval
////                try {
////                    Thread.sleep(10); // Simulate processing time
////                } catch (InterruptedException e) {
////                    log.error("Error during processing: " + e.getMessage());
////                }
////                future.complete(data);
////            }, result -> {
////                if (result.succeeded()) {
////                    // Send the response with data
////                    request.response().end(String.join(",", data));
////                } else {
////                    log.error("Failed to process request: " + result.cause().getMessage());
////                    // Send an error response
////                    request.response().setStatusCode(500).end("Internal Server Error");
////                }
////            });
//        }
//
////        else {
////            log.error("Invalid path requested: " + path);
////            request.response().setStatusCode(404).end("Not Found");
////        }
//    }
//
//    private boolean isServerRunning(int port) {
//        return httpServer.actualPort() == port;
//    }
//
//    public void init(JdbcSinkConfig config) {
//        this.config = config;
//        this.initialized = true;
//        this.start();
//    }
//
//    public boolean isInitialized() {
//        return initialized;
//    }
//
//    public void setInitialized(boolean initialized) {
//        this.initialized = initialized;
//    }
//}
