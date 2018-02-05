package io.confluent.connect.jdbc.source;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Random;

public class TimestampUtils {
  public static Timestamp newTimestamp(String isoDateString, int nanos) {
    try {
      Timestamp result = new Timestamp(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(isoDateString).getTime());
      result.setNanos(nanos);

      return result;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static Timestamp[] getTimestamps(String startingWith, int nanos, int howMany, int gap, int maxJitter) {
    Timestamp[] result = new Timestamp[howMany];
    Timestamp startFrom = newTimestamp(startingWith, nanos);
    result[0] = startFrom;

    for (int i = 1; i < howMany; i++) {
      result[i] = plusDays(startFrom, i, maxJitter);
    }

    return result;
  }

  public static Timestamp plusDays(Timestamp startingFrom, int days, int maxJitter) {
    Timestamp result = new Timestamp(startingFrom.getTime()
        + (days * 1000 * 3600 * 24)
        - (maxJitter > 0 ? new Random().nextInt(maxJitter) : 0));
    result.setNanos(startingFrom.getNanos());

    return result;
  }
}
