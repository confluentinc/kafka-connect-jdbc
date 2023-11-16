package io.confluent.connect.jdbc.util;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ConnectionURLParser {

    private String scheme;

    private String schema;

    private String host;
    private int port;
    private String database;
    private Map<String, String> queryParams;
    private String username;
    private String password;

    public ConnectionURLParser(String jdbcUrl) {
        try {
            if(jdbcUrl == null) {
                throw new IllegalArgumentException("jdbcUrl cannot be null");
            }

          if(jdbcUrl.startsWith("jdbc:")) {
              jdbcUrl = jdbcUrl.substring(5);
          }

            URI uri = new URI(jdbcUrl);

            this.scheme = uri.getScheme();
            this.host = uri.getHost();
            this.port = uri.getPort();

            String query = uri.getQuery();
            this.database = uri.getPath();
            if(this.database != null && this.database.startsWith("/")) {
                this.database = this.database.substring(1);
            }

            // Parse query parameters into a map
            this.queryParams = new java.util.HashMap<>();
            if(query != null) {
                String[] queryParts = query.split("&");
                for (String queryPart : queryParts) {
                    String[] keyValue = queryPart.split("=");
                    if (keyValue.length == 2) {
                        this.queryParams.put(keyValue[0], keyValue[1]);
                    }
                }
            }

            // Extract username and password
            this.username = this.queryParams.get("user");
            this.password = this.queryParams.get("password");
            this.schema = this.queryParams.get("schema");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getScheme() {
        return scheme;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }


    public Map<String, String> getQueryParams() {
        return queryParams;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getSchema() {
        return schema;
    }

    public String getDatabase() {
        return database;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
