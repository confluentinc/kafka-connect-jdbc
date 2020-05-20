/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

//import com.mysql.jdbc.MySQLConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySqlHelper {

  static {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public final String dbName;
  public Connection conn;

  public MySqlHelper(String databaseName) {
    dbName = databaseName;
  }

  public String mySqlUri() {
    return "jdbc:mysql://localhost:3306/" + dbName;
  }

  public void setUp() throws SQLException {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + dbName, "root", "password");
    conn.setAutoCommit(false);
  }

  public void close() throws SQLException, IOException {
    conn.close();
  }

  public void createTable(final String createSql) throws SQLException {
    execute(createSql);
  }

  public void deleteTable(final String table) throws SQLException {
    execute("DROP TABLE IF EXISTS " + table);

    //random errors of table not being available happens in the unit tests
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public int numOfRecordsInDb(String dbName) {
    int count = 0;
    try (Statement stmt = conn.createStatement()) {
      String sql = "SELECT COUNT(*) FROM " + dbName;
      ResultSet rs = stmt.executeQuery(sql);
      while (rs.next()) {
        count++;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return count;
  }

  public int select(final String query, final SqliteHelper.ResultSetReadCallback callback) throws SQLException {
    int count = 0;
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          callback.read(rs);
          count++;
        }
      }
    }
    return count;
  }

  public void execute(String sql) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
      conn.commit();
    }
  }

}

/**
 * //    SqlConf = aMysqldConfig(v5_6_21)
 * //        .withPort(3306)
 * //        .withUser("root", "")
 * //        .build();
 * //
 * //    mysqld = anEmbeddedMysql(SqlConf)
 * //        .addSchema("tests")
 * //        .start();
 * //
 * //    Connection conn = DriverManager.getConnection(
 * //        "jdbc:mysql://localhost:3306/tests", "root", "");
 * //    conn.setAutoCommit(false);
 */
