/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.connect.jdbc.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class SqlParserTest {

  private static final String REDACTED_STRING = "'********'";
  private static final String REDACTED_NUMBER = "0";

  @Test
  public void testRedactStringLiteral() {
    String sql = "SELECT * FROM users WHERE name = 'John Doe'";
    String expected = "SELECT * FROM users WHERE name = " + REDACTED_STRING;
    assertEquals(expected, SqlParser.redact(sql));
  }

  @Test
  public void testRedactLongLiteral() {
    String sql = "SELECT * FROM users WHERE id = 12345";
    String expected = "SELECT * FROM users WHERE id = " + REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redact(sql));
  }

  @Test
  public void testRedactDoubleLiteral() {
    String sql = "SELECT * FROM items WHERE price > 99.99";
    String expected = "SELECT * FROM items WHERE price > " + REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redact(sql));
  }

  @Test
  public void testPositiveSignedNumber() {
    String query = "SELECT * FROM accounts WHERE balance = +1000";
    String result = SqlParser.redact(query);
    String expected = "SELECT * FROM accounts WHERE balance = 0";

    assertFalse(result.contains("+1000"));
    assertFalse(result.contains("1000"));
    assertEquals(expected, result);
  }

  @Test
  public void testNegativeDecimal() {
    String query = "SELECT * FROM transactions WHERE amount = -99.99";
    String result = SqlParser.redact(query);

    assertFalse(result.contains("-99.99"));
    assertFalse(result.contains("99.99"));
    assertEquals("SELECT * FROM transactions WHERE amount = 0", result);
  }

  @Test
  public void testRedactDateLiteral() {
    String sql = "SELECT * FROM orders WHERE order_date = {d '2023-01-01'}";
    String result = SqlParser.redact(sql);
    String expected = "SELECT * FROM orders WHERE order_date = " + REDACTED_STRING;

    assertTrue(result.contains(REDACTED_STRING));
    assertEquals(expected, result);
  }

  @Test
  public void testDateLiteral() {
    String query = "SELECT * FROM orders WHERE order_date = DATE '2024-01-15'";
    String result = SqlParser.redact(query);
    String expected = "SELECT * FROM orders WHERE order_date = " + REDACTED_STRING;

    assertFalse(result.contains("2024-01-15"));
    assertEquals(expected, result);
  }

  @Test
  public void testRedactTimestampLiteral() {
    String sql = "SELECT * FROM logs WHERE timestamp < {ts '2023-01-01 12:00:00'}";
    String result = SqlParser.redact(sql);
    assertTrue(result.contains(REDACTED_STRING));

    assertTrue(result.startsWith("SELECT * FROM logs WHERE timestamp <"));
  }

  @Test
  public void testRedactTimeLiteral() {
    String sql = "SELECT * FROM schedules WHERE start_time = {t '12:00:00'}";
    String result = SqlParser.redact(sql);
    String expected = "SELECT * FROM schedules WHERE start_time = " + REDACTED_STRING;

    assertTrue(result.contains(REDACTED_STRING));
    assertEquals(expected, result);
  }

  @Test
  public void testTimestampLiteral() {
    String query = "SELECT * FROM logs WHERE created_at > TIMESTAMP '2024-01-01 12:30:45'";
    String result = SqlParser.redact(query);
    String expected = "SELECT * FROM logs WHERE created_at > " + REDACTED_STRING;

    assertFalse(result.contains("2024-01-01"));
    assertFalse(result.contains("12:30:45"));
    assertEquals(expected, result);
  }

  @Test
  public void testRedactHexLiteral() {
    String sql = "SELECT * FROM data WHERE bytes = X'DEADBEEF'";
    String result = SqlParser.redact(sql);
    String expected = "SELECT * FROM data WHERE bytes = " + REDACTED_STRING;

    assertTrue(result.contains(REDACTED_STRING));
    assertEquals(expected, result);
  }

  @Test
  public void testMultipleConditions() {
    String sql = "SELECT * FROM users WHERE name = 'Alice' AND age = 30 AND active = 1";
    String expected = "SELECT * FROM users WHERE name = " + REDACTED_STRING
        + " AND age = " + REDACTED_NUMBER + " AND active = " + REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redact(sql));
  }

  @Test
  public void testComplexQueryWithJoinAndSubquery() {
    String sql = "SELECT u.name, o.amount FROM users u " +
                 "JOIN orders o ON u.id = o.user_id " +
                 "WHERE u.status = 'ACTIVE' " +
                 "AND o.amount > (SELECT AVG(amount) FROM orders WHERE region = 'US')";

    String redacted = SqlParser.redact(sql);
    String expected = "SELECT u.name, o.amount FROM users u " +
                      "JOIN orders o ON u.id = o.user_id " +
                      "WHERE u.status = " + REDACTED_STRING + " " +
                      "AND o.amount > (SELECT AVG(amount) FROM orders WHERE region = " + REDACTED_STRING + ")";


    assertTrue(redacted.contains("u.status = " + REDACTED_STRING));
    assertTrue(redacted.contains("region = " + REDACTED_STRING));

    assertEquals(expected, redacted);
  }

  @Test
  public void testInsertStatement() {
    String sql = "INSERT INTO users (id, name, email) VALUES (1, 'Bob', 'bob@example.com')";
    String expected = "INSERT INTO users (id, name, email) VALUES (" + REDACTED_NUMBER + ", "
        + REDACTED_STRING + ", " + REDACTED_STRING + ")";
    System.out.println("Redacted SQL: " + SqlParser.redact(sql));
    assertEquals(expected, SqlParser.redact(sql));
  }

  @Test
  public void testUpdateStatement() {
    String sql = "UPDATE users SET email = 'new@example.com' WHERE id = 100";
    String expected = "UPDATE users SET email = " + REDACTED_STRING + " WHERE id = " + REDACTED_NUMBER;
    System.out.println("Redacted SQL: " + SqlParser.redact(sql));
    assertEquals(expected, SqlParser.redact(sql));
  }

  @Test
  public void testInClause() {
    String sql = "SELECT * FROM products WHERE id IN (1, 2, 3)";
    String expected = "SELECT * FROM products WHERE id IN (" + REDACTED_NUMBER + ", "
        + REDACTED_NUMBER + ", " + REDACTED_NUMBER + ")";
    assertEquals(expected, SqlParser.redact(sql));
  }

  @Test
  public void testInvalidSql() {
    String sql = "This is not a SQL query";
    assertEquals("<redacted>", SqlParser.redact(sql));
  }

  @Test
  public void testEmptyString() {
    assertEquals("", SqlParser.redact(""));
  }
}
