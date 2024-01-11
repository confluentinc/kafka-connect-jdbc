//package io.confluent.connect.jdbc.gp.gpfdist.server;
//
//
//
//        import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
//        import spark.Request;
//        import spark.Response;
//        import spark.Spark;
//
//        import java.util.ArrayList;
//        import java.util.HashMap;
//        import java.util.List;
//        import java.util.Map;
//        import java.util.concurrent.ConcurrentHashMap;
//
//public class SparkHttpServer implements IGpfdistServer{
//
//    private static final SparkHttpServer INSTANCE = new SparkHttpServer();
//
//    private final Map<String, List<String>> pathDataMap;
//    private JdbcSinkConfig config;
//    private boolean initialized;
//
//    private SparkHttpServer() {
//        this.pathDataMap = new ConcurrentHashMap<>();
//    }
//
//    public static SparkHttpServer getInstance() {
//        return INSTANCE;
//    }
//
//    public void start() {
//        int port = config.getGpfdistPort();
////        if (isServerRunning(port)) {
////            throw new IllegalStateException("Server is already running on port " + port);
////        }
//        Spark.port(port);
//        // Set up routes
//        Spark.get("/status", this::getStatus);
//        Spark.get("*", this::handleRequest);
//
//        Spark.awaitInitialization();
//    }
//
//    @Override
//    public void stop() {
//        Spark.stop();
//    }
//
//    @Override
//    public void addData(String path, List<List<String>> data) {
//        pathDataMap.put(path, data.get(0));
//    }
//
//    @Override
//    public void addData(String path, String data) {
//
//    }
//
//    public String getStatus(Request request, Response response) {
//        response.type("application/json");
//
//        return "{\"status\": \"Server is running\"}";
//    }
//
//    public String handleRequest(Request request, Response response) {
//        String path = request.pathInfo();
//        List<String> data = pathDataMap.getOrDefault(path, new ArrayList<>());
//
//        if (data.isEmpty()) {
//            response.status(404);
//            return "Not Found";
//        }
//
//        response.type("text/plain");
//        response.header("Expires", "0");
//        response.header("X-GPFDIST-VERSION", "1.0.0");
//        response.header("X-GP-PROTO", "1");
//        response.header("Cache-Control", "no-cache");
//        response.header("Connection", "close");
//
//        return String.join(",", data);
//    }
//
//    private boolean isServerRunning(int port) {
//        return Spark.port() == port;
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
