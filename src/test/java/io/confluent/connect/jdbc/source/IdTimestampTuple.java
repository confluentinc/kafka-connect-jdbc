package io.confluent.connect.jdbc.source;

import java.sql.Timestamp;
import java.util.Objects;

public class IdTimestampTuple {
  public final int id;
  public final Timestamp timestamp;

  public IdTimestampTuple(int id, Timestamp timestamp) {
    this.id = id;
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IdTimestampTuple tuple = (IdTimestampTuple) o;
    return id == tuple.id &&
        Objects.equals(timestamp, tuple.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, timestamp);
  }


  @Override
  public String toString() {
    return "IdTimestampTuple{" +
        "id=" + id +
        ", timestamp=" + timestamp +
        '}';
  }
}
