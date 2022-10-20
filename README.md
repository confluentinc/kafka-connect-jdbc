# 수정된 Kafka Connect JDBC Connector

이 저장소는 수정된 kafka JDBC Source Connector (이하 JDBC 커넥터) 를 담고 있다.

## 수정의 이유

- DB 에 로그를 남기는 서비스에서는 날짜 단위로 테이블을 로테이션해주는 경우가 많다.
- 원본 JDBC 커넥터는 이런 경우를 위한 지원이 없다.
- 이에 쿼리에 테이블 로테이션을 지원하기 위한 몇 가지 기능을 추가

## 수정된 내용
수정은 향후 업데이트시 작업을 최소화 하기 위해 가급적 원본에서 작은 범위의 수정만 가하도록 하였다.

### 동적 날짜 생성

일반적인 쿼리에 동적으로 변하는 날짜를 지원하기 위해 아래와 같은 매크로 기능이 추가되었다. 실제 쿼리 실행시는 `{{ }}` 로 표시된 매크로가 구체적인 문자열로 바뀌어 실행된다 (현재는 `incrementing` 또는 `timestamp+incrementing` 모드에서 이용 가능).

- `DayAddFmt` 현재 일시에서 주어진 날짜를 더한 후 포맷을 통해 문자열로 바꾼다 (음의 정수를 지정하면 빼는 효과).
  - 예: `SELECT * FROM log_{{ DayAddFmt -1 yyyyMMdd }}`
  -  결과 (현재 2022-09-08 인 경우) : `SELECT * FROM log_20220907`
- `DayAddFmtDelay` 분 단위로 지연된 현재 일시에서 주어진 날짜를 더한 후 포맷을 통해 문자열로 바꾼다 (테이블 로테이션이 자정 이후 일정 시간 지연되어 수행되는 경우 이용).
    - 예: `SELECT * FROM log_{{ DayAddFmtDelay -1 yyyyMMdd 30 }}`
    - 결과 (현재 2022-09-08 인 경우)
      - 0 시에서 30분 미만 지난 경우 : `SELECT * FROM log_20220906`
      - 0 시에서 30분 이상 지난 경우 : `SELECT * FROM log_20220907`

### 폴백 쿼리 

위의 동적 날짜 생성을 이용해 로테이션된 테이블을 쿼리했을 때 해당 테이블이 로테이션 지연 등의 이유로 아직 존재하지 않으면 커넥터에서 에러가 발생한다. 이렇게 되면 커넥터는 잠시 후 다시 시도하게 되는데, 이 과정에서 오프셋이 커밋되지 못해 메시지 중복이 발생하게 된다.

이런 경우 폴백 쿼리 (Fallback Query) 를 이용할 수 있다. 폴백 쿼리는 기본 쿼리 아래 `-----` (대쉬 `-` 5 개) 를 구분자로 하여 기술한다. 아래는 동적 날짜 생성과 폴백 쿼리를 함께 사용하는 MSSQL 용 쿼리의 예이다.

```sql
SELECT * FROM 
(
    SELECT * FROM log_{{ DayAddFmt -1 yyyyMMdd }}
    UNION ALL
    SELECT * FROM log
) AS T
-----
SELECT CONVERT(DATETIME, '1971-01-01 00:00:01.000') AS RegDate AS T
```

위 쿼리를 API 를 통한 JDBC 소스 커넥터 등록시 `query` 의 값으로 주면, 먼저 위쪽의 동적 날짜 생성 매크로가 있는 쿼리 (기본 쿼리) 를 시도하고, 그것이 실패하면 아래쪽의 쿼리 (폴백 쿼리) 를 실행하는 식이다.

예제는 타임스탬프 모드에서 `RegDate` 컬럼을 사용하는 것을 가정한 폴백 쿼리인데, 이렇게 하면 테이블 로테이션이 늦어져도 커넥터에서 에러가 발생하지 않기에 필요없는 메시지 중복이 발생하지 않는다. 

## 설치용 파일

[릴리즈 페이지](https://github.com/haje01/kafka-connect-jdbc/releases)에서 압축 파일을 내려 받아 Kafka 의 `connectors` 디렉토리에 설치 후 사용한다. 자세한 것은 Confluent 문서의 [Install the connector manually](https://docs.confluent.io/kafka-connectors/jdbc/current/index.html#install-the-connector-manually) 부분을 참고한다.

### 직접 설치용 빌드 만들기
이 저장소에서 릴리즈되는 압축 파일을 사용하면 편리하지만, 만약 직접 빌드가 필요한 경우를 위해 관련 내용을 설명한다. 나의 경우 무료인 IntelliJ IDEA (커뮤니티 버전, 2022.1) 을 사용하고 있기에, 그것을 기준으로 설명하겠다.

- 코드를 받은 뒤 IntelliJ 에서 `File / Open..` 메뉴를 선택
- 코드 디렉토를 선택 후 `OK`
- IntelliJ 가 의존성 패키지를 체크하여 받는다.
- `Build / Build Project` 를 하면 빌드를 수행하다 경고가 나오는데 `-Werror` 옵션으로 인해 빌드 실패가 된다.
- `File / Settings` 메뉴로 IDE 설정창을 열고 왼쪽의 `Build, Execution, Deployment / Compiler / Java Compiler` 트리를 선택
- `Override compiler parameters per module` 파트에서 `kafka-connector-jdbc` 항목의 `Compilation options` 에서 `-Werror` 을 제거한다.
- 다시 `Build / Build Project` 를 해보면 몇 가지 경고는 나오지만 빌드는 성공한다.
- 왼쪽의 프로젝트 최상위 폴더에서 오른 클릭 후 나오는 `Open Module Setting` 메뉴 선택
- 표시되는 `Project Structure` 대화창에서 `Project Settings / Artifacts` 선택
- `+` 버튼을 누른 뒤 `JAR / From modules with dependencies` 메뉴 선택
- 표시되는 `Create JAR from Modules` 대화창에서 `JAR files from libraries` 를 `copy to the output directory and link via manifest` 를 선택
- 이후 나오는 대화 창의 Name `kafka-connect-jdbc:jar` 에서 `:jar` 부분 삭제
- `Include in project build` 체크
- `Output Directory` 경로 확인 후 `OK` 누름
- 이제 프로젝트를 빌드하면 자동으로 아카이브가 만들어지고, 아까 확인한 `Output Directory` 아래 `out/artifacts` 디렉토리로 가서 생성된 `kafka-connect-jdbc` 디렉토리 확인
- 이 디렉토리를 `.zip` 으로 압축하여 배포 (필요시 파일명에 원본 버전 명기)

## 원본 JDBC 커넥터가 업데이트된 경우 패치할 것

원본 JDBC 커넥터가 갱신되었고, 그것을 반영해야하는 경우 `git pull 후` 다음과 같이 진행한다.

먼저 [원본 저장소에서 싱크하기](https://stackoverflow.com/questions/7244321/how-do-i-update-or-sync-a-forked-repository-on-github) 를 참고하여 원본 JDBC 커넥터의 최신본을 받아온 후 `TimestampIncrementingTableQuerier.java` 코드를 아래와 같이 수정한다.

```diff
diff --git a/src/main/java/io/confluent/connect/jdbc/source/TableQuerier.java b/src/main/java/io/confluent/connect/jdbc/source/TableQuerier.java
index 25fcf155..77774c86 100644
--- a/src/main/java/io/confluent/connect/jdbc/source/TableQuerier.java
+++ b/src/main/java/io/confluent/connect/jdbc/source/TableQuerier.java
@@ -23,6 +23,8 @@ import java.sql.Connection;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.SQLException;
+import java.util.regex.Matcher;
+import java.util.regex.Pattern;
 
 import io.confluent.connect.jdbc.dialect.DatabaseDialect;
 import io.confluent.connect.jdbc.util.ExpressionBuilder;
@@ -43,7 +45,8 @@ abstract class TableQuerier implements Comparable<TableQuerier> {
 
   protected final DatabaseDialect dialect;
   protected final QueryMode mode;
-  protected final String query;
+  protected String query;
+  protected final String fbquery;
   protected final String topicPrefix;
   protected final TableId tableId;
   protected final String suffix;
@@ -53,6 +56,7 @@ abstract class TableQuerier implements Comparable<TableQuerier> {
   protected long lastUpdate;
   protected Connection db;
   protected PreparedStatement stmt;
+  protected PreparedStatement fbstmt;
   protected ResultSet resultSet;
   protected SchemaMapping schemaMapping;
   private String loggedQueryString;
@@ -69,13 +73,33 @@ abstract class TableQuerier implements Comparable<TableQuerier> {
     this.dialect = dialect;
     this.mode = mode;
     this.tableId = mode.equals(QueryMode.TABLE) ? dialect.parseTableIdentifier(nameOrQuery) : null;
-    this.query = mode.equals(QueryMode.QUERY) ? nameOrQuery : null;
+    String[] queries = splitFallbackQuery(nameOrQuery);
+    this.query = mode.equals(QueryMode.QUERY) ? queries[0] : null;
+    this.fbquery = mode.equals(QueryMode.QUERY) ? queries[1] : null;
+    if (this.fbquery != null && this.fbquery.length() > 0) {
+      log.warn("Found fallback query: " + this.fbquery);
+    }
     this.topicPrefix = topicPrefix;
     this.lastUpdate = 0;
     this.suffix = suffix;
     this.attemptedRetries = 0;
   }
 
+  protected String[] splitFallbackQuery(String query) {
+    Pattern p = Pattern.compile("(.*)\\s+-----\\s+(.*)$", Pattern.DOTALL | Pattern.MULTILINE);
+    Matcher m = p.matcher(query);
+    String[] queries = new String[2];
+    if (m.matches()) {
+      queries[0] = m.group(1);
+      queries[1] = m.group(2);
+    }
+    else {
+      queries[0] = query;
+      queries[1] = "";
+    }
+    return queries;
+  }
+
   public long getLastUpdate() {
     return lastUpdate;
   }
@@ -84,7 +108,18 @@ abstract class TableQuerier implements Comparable<TableQuerier> {
     if (stmt != null) {
       return stmt;
     }
+    // Create base query statement
+    createPreparedStatement(db);
+
+    // Create fallback query statement
+    PreparedStatement ostmt = stmt;
+    String oquery = query;
+    query = fbquery;
     createPreparedStatement(db);
+    fbstmt = stmt;
+    query = oquery;
+    stmt = ostmt;
+
     return stmt;
   }
 
diff --git a/src/main/java/io/confluent/connect/jdbc/source/TimestampIncrementingTableQuerier.java b/src/main/java/io/confluent/connect/jdbc/source/TimestampIncrementingTableQuerier.java
index 11d1217f..54dd1408 100644
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
 
+  private String prevRenderedQuery;
+
   public TimestampIncrementingTableQuerier(DatabaseDialect dialect, QueryMode mode, String name,
                                            String topicPrefix,
                                            List<String> timestampColumnNames,
@@ -115,6 +120,7 @@ public class TimestampIncrementingTableQuerier extends TableQuerier implements C
 
     this.timeZone = timeZone;
     this.timestampGranularity = timestampGranularity;
+    this.prevRenderedQuery = "";
   }
 
   /**
@@ -122,6 +128,58 @@ public class TimestampIncrementingTableQuerier extends TableQuerier implements C
    */
   private static String DATETIME = "datetime";
 
+  /**
+   * 매크로가 포함된 쿼리를 렌더링
+   */
+  protected String renderQuery(String qry) {
+    Pattern p = Pattern.compile("(.*)\\{\\{(.*)\\}\\}(.*)$", Pattern.DOTALL | Pattern.MULTILINE);
+    while (true) {
+      Matcher m = p.matcher(qry);
+      if (m.matches()) {
+        String head = m.group(1);
+        String macro = m.group(2);
+        String tail = m.group(3);
+        String[] elms = macro.trim().split(" ");
+        int delta = Integer.parseInt(elms[1]);
+        DateTimeFormatter fmt = DateTimeFormatter.ofPattern(elms[2]);
+        ZonedDateTime now = ZonedDateTime.now();
+        String cmd = elms[0].trim();
+        switch (cmd) {
+          case "DayAddFmt":
+            macro = now.plusDays(delta).format(fmt);
+            break;
+          case "DayAddFmtDelay":
+            int minute_delay = Integer.parseInt(elms[3]);
+            macro = now.minusMinutes(minute_delay).plusDays(delta).format(fmt);
+            break;
+          case "HourAddFmt":
+            macro = now.plusHours(delta).format(fmt);
+            break;
+          case "MinAddFmt":
+            macro = now.plusMinutes(delta).format(fmt);
+            break;
+          case "MinAddFmtDelay":
+            int second_delay = Integer.parseInt(elms[3]);
+            macro = now.minusSeconds(second_delay).plusMinutes(delta).format(fmt);
+            break;
+          default:
+            assert false;
+        }
+        qry = head + macro + tail;
+      }
+      else
+        break;
+    }
+    if (!prevRenderedQuery.equals(qry)) {
+      log.warn("invalidate query cache.");
+      stmt = null;
+      prevRenderedQuery = qry;
+      log.warn("renderedQuery: " + qry);
+    }
+
+    return qry;
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
 
@@ -206,8 +265,25 @@ public class TimestampIncrementingTableQuerier extends TableQuerier implements C
   @Override
   protected ResultSet executeQuery() throws SQLException {
     criteria.setQueryParameters(stmt, this);
+    criteria.setQueryParameters(fbstmt, this);
     log.trace("Statement to execute: {}", stmt.toString());
-    return stmt.executeQuery();
+    try {
+      return stmt.executeQuery();
+    }
+    catch(SQLException e) {
+      log.warn("Base query exception: " + e);
+      if (fbquery.length() > 0) {
+        log.warn("Try fallback query.");
+        try {
+          return fbstmt.executeQuery();
+        } catch(SQLException e2) {
+          log.warn("Fallback query exception: " + e2);
+          throw e2;
+        }
+      } else {
+        throw e;
+      }
+    }
   }
 
   @Override
```

> 새로운 버전을 적용한 뒤에는 `version.txt` 파일에 적용한 버전을 명기하도록 하자.

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

