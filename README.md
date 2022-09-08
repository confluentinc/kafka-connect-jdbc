# 수정된 Kafka Connect JDBC Connector

이 저장소는 수정된 kafka JDBC Source Connector (이하 JDBC 커넥터) 를 담고 있다.

## 수정의 이유 

- DB 에 로그를 남기는 서비스에서는 날짜 단위로 테이블을 로테이션해주는 경우가 많다.
- 원본 JDBC 커넥터는 이런 경우를 위한 지원이 약하다.
- 이에 테이블 로테이션을 지원하기 위한 간단한 매크로 기능을 도입하였다. 

## 수정된 내용

일반적인 쿼리에 아래와 같은 매크로 기능을 사용할 수 있다 (현재는 `timestamp+incrementing` 모드에서만 가능하다).

- `DayAddFmt` 현재 일시에서 날짜를 더한 후 포맷을 통해 문자열로 바꾼다.
  - 예: `SELECT * FROM log_{{ DayAddFmt -1 yyyyMMdd }}`
  -  결과 (현재 2022-09-08 인경우) : `SELECT * FROM log_20220907`
- `DayAddFmtDelay` 지연된 현재 일시에서 날짜를 더한 후 포맷을 통해 문자열로 바꾼다.
    - 예: `SELECT * FROM log_{{ DayAddFmtDelay -1 yyyyMMdd 30 }}`
    -  결과 (현재 2022-09-08 에서 30분 이상 지난 경우) : `SELECT * FROM log_20220907`

## 원본 JDBC 커넥터가 업데이트된 경우 패치할 것 

원본 JDBC 커넥터가 갱신되었고, 그것을 반영해야하는 경우 `git pull 후` 다음과 같이 진행한다.

먼저 [원본 저장소에서 싱크하기](https://stackoverflow.com/questions/7244321/how-do-i-update-or-sync-a-forked-repository-on-github) 를 참고하여 원본 JDBC 커넥터의 최신본을 받아온 후 `TimestampIncrementingTableQuerier.java` 코드를 아래와 같이 수정한다. 

```diff
diff --git a/src/main/java/io/confluent/connect/jdbc/source/TimestampIncrementingTableQuerier.java b/src/main/java/io/confluent/connect/jdbc/source/TimestampIncrementingTableQuerier.java
index 11d1217f..2185f6a0 100644
--- a/src/main/java/io/confluent/connect/jdbc/source/TimestampIncrementingTableQuerier.java
+++ b/src/main/java/io/confluent/connect/jdbc/source/TimestampIncrementingTableQuerier.java
@@ -33,6 +33,9 @@ import java.util.Collections;
 import java.util.TimeZone;
 import java.util.List;
 import java.util.Map;
+import java.util.regex.*;
+import java.time.ZonedDateTime;
+import java.time.format.DateTimeFormatter;

 import io.confluent.connect.jdbc.dialect.DatabaseDialect;
 import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TimestampGranularity;
@@ -77,6 +80,8 @@ public class TimestampIncrementingTableQuerier extends TableQuerier implements C
   private final long timestampDelay;
   private final TimeZone timeZone;

+  private String prevMacroResult;
+
   public TimestampIncrementingTableQuerier(DatabaseDialect dialect, QueryMode mode, String name,
                                            String topicPrefix,
                                            List<String> timestampColumnNames,
@@ -115,6 +120,7 @@ public class TimestampIncrementingTableQuerier extends TableQuerier implements C

     this.timeZone = timeZone;
     this.timestampGranularity = timestampGranularity;
+    this.prevMacroResult = "";
   }

   /**
@@ -122,6 +128,58 @@ public class TimestampIncrementingTableQuerier extends TableQuerier implements C
    */
   private static String DATETIME = "datetime";

+  /**
+   * 매크로가 포함된 쿼리를 렌더링
+   */
+  protected String renderQuery(String query) {
+    Pattern p = Pattern.compile("(.*)\\{\\{(.*)\\}\\}(.*)$", Pattern.DOTALL | Pattern.MULTILINE);
+    Matcher m = p.matcher(query);
+    if (m.matches()) {
+      String head = m.group(1);
+      String macro = m.group(2);
+      String tail = m.group(3);
+      String []elms = macro.trim().split(" ");
+      int delta = Integer.parseInt(elms[1]);
+      DateTimeFormatter fmt = DateTimeFormatter.ofPattern(elms[2]);
+      ZonedDateTime now = ZonedDateTime.now();
+      switch (elms[0]) {
+        // 날짜 증가 후 포매팅
+        case "DayAddFmt":
+          macro = now.plusDays(delta).format(fmt);
+          break;
+        // 지연이 있는 날짜 증가 후 포매팅
+        case "DayAddFmtDelay":
+          int minute_delay = Integer.parseInt(elms[3]);
+          macro = now.minusMinutes(minute_delay).plusDays(delta).format(fmt);
+          break;
+        // 시간 증가 후 포매팅
+        case "HourAddFmt":
+          macro = now.plusHours(delta).format(fmt);
+          break;
+        // 분 증가 후 포매팅
+        case "MinAddFmt":
+          macro = now.plusMinutes(delta).format(fmt);
+          break;
+        // 지연이 있는 분 증가 후 포매팅
+        case "MinAddFmtDelay":
+          int second_delay = Integer.parseInt(elms[3]);
+          macro = now.minusSeconds(second_delay).plusMinutes(delta).format(fmt);
+          break;
+        default:
+          assert false;
+      }
+      query = head + macro + tail;
+      // 매크로 결과가 이전과 다르면 쿼리 캐쉬 무효화
+      if (prevMacroResult != macro) {
+        log.warn("invalidate query cache.");
+        stmt = null;
+        prevMacroResult = macro;
+      }
+    }
+    log.warn("renderedQuery: ", query.toString());
+    return query;
+  }
+
   @Override
   protected void createPreparedStatement(Connection db) throws SQLException {
     findDefaultAutoIncrementingColumn(db);
@@ -138,7 +196,7 @@ public class TimestampIncrementingTableQuerier extends TableQuerier implements C
         builder.append(tableId);
         break;
       case QUERY:
-        builder.append(query);
+        builder.append(renderQuery(query));
         break;
       default:
         throw new ConnectException("Unknown mode encountered when preparing query: " + mode);
@@ -153,6 +211,7 @@ public class TimestampIncrementingTableQuerier extends TableQuerier implements C
     String queryString = builder.toString();
     recordQuery(queryString);
     log.trace("{} prepared SQL query: {}", this, queryString);
+
     stmt = dialect.createPreparedStatement(db, queryString);
   }
```

아래는 원본의 설명.
----
# Kafka Connect JDBC Connector

kafka-connect-jdbc is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data to and from any JDBC-compatible database.

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-jdbc/docs/index.html).

# Development

To build a development version you'll need a recent version of Kafka as well as a set of upstream Confluent projects, which you'll have to build from their appropriate snapshot branch. See the [FAQ](https://github.com/confluentinc/kafka-connect-jdbc/wiki/FAQ)
for guidance on this process.

You can build kafka-connect-jdbc with Maven using the standard lifecycle phases.

# FAQ

Refer frequently asked questions on Kafka Connect JDBC here -
https://github.com/confluentinc/kafka-connect-jdbc/wiki/FAQ

# Contribute

Contributions can only be accepted if they contain appropriate testing. For example, adding a new dialect of JDBC will require an integration test.

- Source Code: https://github.com/confluentinc/kafka-connect-jdbc
- Issue Tracker: https://github.com/confluentinc/kafka-connect-jdbc/issues
- Learn how to work with the connector's source code by reading our [Development and Contribution guidelines](CONTRIBUTING.md).

# Information

For more information, check the documentation for the JDBC connector on the [confluent.io](https://docs.confluent.io/current/connect/kafka-connect-jdbc/index.html) website. Questions related to the connector can be asked on [Community Slack](https://launchpass.com/confluentcommunity) or the [Confluent Platform Google Group](https://groups.google.com/forum/#!topic/confluent-platform/).

# License

This project is licensed under the [Confluent Community License](LICENSE).

