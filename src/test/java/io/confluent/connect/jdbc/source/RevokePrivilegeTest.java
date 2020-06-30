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
package io.confluent.connect.jdbc.source;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;

public class RevokePrivilegeTest {
  String TABLE_NAME = "testTable";
  Connection connection, connection1;

  @Before
  public void setUp() throws SQLException {
    connection = getConnection();
  }

  @Test
  public void revokeReadPrivilegeWithConnectionTest() throws SQLException {
    dropTableIfExists(TABLE_NAME);
    sendTestDataToMysql(0, 10);
    revokeReadPrivileges();
    int count = loadFromSQL(TABLE_NAME, connection);
    Assert.assertEquals(10, count);
  }

  @Test
  public void revokeReadPrivilegeWithConnection1Test() throws SQLException {
    grantAllPrivileges();
    dropTableIfExists(TABLE_NAME);
    sendTestDataToMysql(0, 10);
    connection1 = getConnection1();
    int count = loadFromSQL(TABLE_NAME, connection1);
    Assert.assertEquals(10, count);
    revokeReadPrivileges();
    count = loadFromSQL(TABLE_NAME, connection1);
    Assert.assertEquals(10, count);
    connection1.close();
  }

  @Test(expected = SQLSyntaxErrorException.class)
  public void revokeReadPrivilegeWithNewConnection1Test() throws SQLException {
    grantAllPrivileges();
    dropTableIfExists(TABLE_NAME);
    sendTestDataToMysql(0, 10);
    connection1 = getConnection1();
    int count = loadFromSQL(TABLE_NAME, connection1);
    Assert.assertEquals(10, count);
    revokeReadPrivileges();
    connection1 = getConnection1();
    count = loadFromSQL(TABLE_NAME, connection1);
    Assert.assertEquals(10, count);
    connection1.close();
  }

  private void grantAllPrivileges() throws SQLException {
    Statement st = connection.createStatement();
    st.executeQuery("grant all privileges ON db.* TO user@'%'");
  }

  private int loadFromSQL(String mySqlTable, Connection conn) throws SQLException {
    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT COUNT(*) AS rowcount FROM " + mySqlTable);
    rs.next();
    return rs.getInt("rowcount");
  }

  private void revokeWritePrivilege() throws SQLException {
    Statement st = connection.createStatement();
    st.executeQuery("REVOKE INSERT ON db.* FROM user@'%'");
  }

  private void revokeReadPrivileges() throws SQLException {
    Statement st = connection.createStatement();
    st.executeQuery("revoke select on db.* from user@'%'");
  }

  protected Connection getConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:mysql://localhost:3306/db" , "root", "password");
  }

  protected Connection getConnection1() throws SQLException {
    return DriverManager.getConnection("jdbc:mysql://localhost:3306/db" , "user", "password");
  }

  private void sendTestDataToMysql(int idFrom, int idTo) throws SQLException {
    Statement st = connection.createStatement();
    String sql;
    if (!tableExist(connection, TABLE_NAME)) {
      sql = "CREATE TABLE " + TABLE_NAME +
          "(id INTEGER not NULL, " +
          " first_name VARCHAR(255), " +
          " last_name VARCHAR(255), " +
          " age INTEGER, " +
          " PRIMARY KEY ( id ))";
      st.executeQuery(sql);
    }
    sql = "INSERT INTO testTable(id,first_name,last_name,age) "
        + "VALUES(?,?,?,?)";
    PreparedStatement pstmt = connection.prepareStatement(sql);
    for (int i = idFrom; i<idTo; i++) {
      pstmt.setLong(1, i);
      pstmt.setString(2, "FirstName");
      pstmt.setString(3, "LastName");
      pstmt.setLong(4, 20);
      pstmt.executeUpdate();
    }
    pstmt.close();
  }

  private void dropTableIfExists(String kafkaTopic) throws SQLException {
    Statement st = connection.createStatement();
    st.executeQuery("drop table if exists " + kafkaTopic);
  }

  public static boolean tableExist(Connection conn, String tableName) throws SQLException {
    boolean tExists = false;
    try (ResultSet rs = conn.getMetaData().getTables(null, null, tableName, null)) {
      while (rs.next()) {
        String tName = rs.getString("TABLE_NAME");
        if (tName != null && tName.equals(tableName)) {
          tExists = true;
          break;
        }
      }
    }
    return tExists;
  }
}
