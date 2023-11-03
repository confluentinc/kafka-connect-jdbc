package io.confluent.connect.jdbc.util;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ConnectionURLParser {

    private String scheme;

    private String schema;

    private String host;
    private int port;
    private String path;
    private Map<String, String> queryParams;
    private String username;
    private String password;

    public ConnectionURLParser(String jdbcUrl) {
        try {
            URI uri = new URI(jdbcUrl);

            this.scheme = uri.getScheme();
            this.host = uri.getHost();
            this.port = uri.getPort();

            String query = uri.getQuery();
            this.path = uri.getPath();
            if(this.path != null && this.path.startsWith("/")) {
                this.path = this.path.substring(1);
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

    public String getPath() {
        return path;
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


    public void setSchema(String schema) {
        this.schema = schema;
    }
}
