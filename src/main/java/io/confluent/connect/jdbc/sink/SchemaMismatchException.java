package io.confluent.connect.jdbc.sink;

import java.sql.SQLException;

public class SchemaMismatchException extends SQLException {

  public SchemaMismatchException(String reason) {
    super(reason);
  }

  public SchemaMismatchException(String reason, Throwable cause) {
    super(reason, cause);
  }
}

