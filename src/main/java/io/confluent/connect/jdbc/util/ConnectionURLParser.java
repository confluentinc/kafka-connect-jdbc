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
    public String getQualifiedDatabase() {
        return schema!=null?schema+"."+database:database;
    }


    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUrl() {
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append("jdbc:").append(this.scheme).append("://");

        if (this.host != null) {
            urlBuilder.append(this.host);
        }
        if (this.port > 0) {
            urlBuilder.append(":").append(this.port);
        }
        if (this.database != null) {
            urlBuilder.append("/").append(this.database);
        }

        boolean hasParams = this.username != null || this.password != null || this.schema != null;
        if (hasParams) {
            urlBuilder.append("?");
            boolean first = true;
            if (this.schema != null) {
                urlBuilder.append("schema=").append(this.schema);
                first = false;
            }
            if (this.username != null) {
                if (!first) urlBuilder.append("&");
                urlBuilder.append("user=").append(this.username);
                first = false;
            }
            if (this.password != null) {
                if (!first) urlBuilder.append("&");
                urlBuilder.append("password=").append(this.password);
            }
        }

        return urlBuilder.toString();
    }

}
