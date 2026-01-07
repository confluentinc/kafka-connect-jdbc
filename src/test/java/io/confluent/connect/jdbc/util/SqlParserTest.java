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

import net.sf.jsqlparser.JSQLParserException;
import org.junit.Test;

import static org.junit.Assert.*;

public class SqlParserTest {

  @Test
  public void testValidateSqlSyntaxNullNoThrow() throws JSQLParserException {
    SqlParser.validateSqlSyntax(null);
  }

  @Test
  public void testValidateSqlSyntaxEmptyNoThrow() throws JSQLParserException {
    SqlParser.validateSqlSyntax("");
    SqlParser.validateSqlSyntax("   ");
  }

  @Test
  public void testValidateSqlSyntaxValidSelectNoThrow() throws JSQLParserException {
    SqlParser.validateSqlSyntax("SELECT 1");
    SqlParser.validateSqlSyntax("SELECT * FROM users WHERE id = 1");
  }

  @Test(expected = JSQLParserException.class)
  public void testValidateSqlSyntaxInvalidSqlThrowsJSQLParserException() throws JSQLParserException {
    // Intentionally malformed SQL (common user mistake)
    SqlParser.validateSqlSyntax("SELECT FROM");
  }

  @Test
  public void testRedactStringLiteral() {
    String sql = "SELECT * FROM users WHERE name = 'John Doe'";
    String expected = "SELECT * FROM users WHERE name = " + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testRedactLongLiteral() {
    String sql = "SELECT * FROM users WHERE id = 12345";
    String expected = "SELECT * FROM users WHERE id = " + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testRedactDoubleLiteral() {
    String sql = "SELECT * FROM items WHERE price > 99.99";
    String expected = "SELECT * FROM items WHERE price > " + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPositiveSignedNumber() {
    String query = "SELECT * FROM accounts WHERE balance = +1000";
    String result = SqlParser.redactSensitiveData(query);
    String expected = "SELECT * FROM accounts WHERE balance = 0";

    assertFalse(result.contains("+1000"));
    assertFalse(result.contains("1000"));
    assertEquals(expected, result);
  }

  @Test
  public void testNegativeDecimal() {
    String query = "SELECT * FROM transactions WHERE amount = -99.99";
    String result = SqlParser.redactSensitiveData(query);
    String expected = "SELECT * FROM transactions WHERE amount = 0";

    assertFalse(result.contains("-99.99"));
    assertFalse(result.contains("99.99"));
    assertEquals(expected, result);
  }

  @Test
  public void testRedactDateLiteral() {
    String sql = "SELECT * FROM orders WHERE order_date = {d '2023-01-01'}";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM orders WHERE order_date = " + SqlParser.REDACTED_STRING;

    assertTrue(result.contains(SqlParser.REDACTED_STRING));
    assertEquals(expected, result);
  }

  @Test
  public void testDateLiteral() {
    String query = "SELECT * FROM orders WHERE order_date = DATE '2024-01-15'";
    String result = SqlParser.redactSensitiveData(query);
    String expected = "SELECT * FROM orders WHERE order_date = " + SqlParser.REDACTED_STRING;

    assertFalse(result.contains("2024-01-15"));
    assertEquals(expected, result);
  }

  @Test
  public void testRedactTimestampLiteral() {
    String sql = "SELECT * FROM logs WHERE timestamp < {ts '2023-01-01 12:00:00'}";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM logs WHERE timestamp < " + SqlParser.REDACTED_STRING;

    assertTrue(result.contains(SqlParser.REDACTED_STRING));
    assertEquals(expected, result);
  }

  @Test
  public void testRedactTimeLiteral() {
    String sql = "SELECT * FROM schedules WHERE start_time = {t '12:00:00'}";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM schedules WHERE start_time = " + SqlParser.REDACTED_STRING;

    assertTrue(result.contains(SqlParser.REDACTED_STRING));
    assertEquals(expected, result);
  }

  @Test
  public void testTimestampLiteral() {
    String query = "SELECT * FROM logs WHERE created_at > TIMESTAMP '2024-01-01 12:30:45'";
    String result = SqlParser.redactSensitiveData(query);
    String expected = "SELECT * FROM logs WHERE created_at > " + SqlParser.REDACTED_STRING;

    assertFalse(result.contains("2024-01-01"));
    assertFalse(result.contains("12:30:45"));
    assertEquals(expected, result);
  }

  @Test
  public void testRedactHexLiteral() {
    String sql = "SELECT * FROM data WHERE bytes = X'DEADBEEF'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM data WHERE bytes = " + SqlParser.REDACTED_STRING;

    assertTrue(result.contains(SqlParser.REDACTED_STRING));
    assertEquals(expected, result);
  }

  @Test
  public void testMultipleConditions() {
    String sql = "SELECT * FROM users WHERE name = 'Alice' AND age = 30 AND active = 1";
    String expected = "SELECT * FROM users WHERE name = " + SqlParser.REDACTED_STRING
        + " AND age = " + SqlParser.REDACTED_NUMBER + " AND active = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testComplexQueryWithJoinAndSubquery() {
    String sql = "SELECT u.name, o.amount FROM users u " +
                 "JOIN orders o ON u.id = o.user_id " +
                 "WHERE u.status = 'ACTIVE' " +
                 "AND o.amount > (SELECT AVG(amount) FROM orders WHERE region = 'US')";

    String redacted = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT u.name, o.amount FROM users u " +
                      "JOIN orders o ON u.id = o.user_id " +
                      "WHERE u.status = " + SqlParser.REDACTED_STRING + " " +
                      "AND o.amount > (SELECT AVG(amount) FROM orders WHERE region = " + SqlParser.REDACTED_STRING + ")";


    assertTrue(redacted.contains("u.status = " + SqlParser.REDACTED_STRING));
    assertTrue(redacted.contains("region = " + SqlParser.REDACTED_STRING));
    assertEquals(expected, redacted);
  }

  @Test
  public void testUpdateStatement() {
    String sql = "UPDATE users SET email = 'new@example.com' WHERE id = 100";
    String expected = "UPDATE users SET email = " + SqlParser.REDACTED_STRING + " WHERE id = " + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testInClause() {
    String sql = "SELECT * FROM products WHERE id IN (1, 2, 3)";
    String expected = "SELECT * FROM products WHERE id IN (" + SqlParser.REDACTED_NUMBER + ", "
        + SqlParser.REDACTED_NUMBER + ", " + SqlParser.REDACTED_NUMBER + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testEmptyString() {
    assertEquals("", SqlParser.redactSensitiveData(""));
  }

  // ========================================================================================
  // Multiple-database Pattern Tests
  // ========================================================================================

  @Test
  public void testComplexNestedSubqueries() {
    String sql = "SELECT * FROM orders o WHERE o.customer_id IN " +
                 "(SELECT c.id FROM customers c WHERE c.region = 'APAC' " +
                 "AND c.credit_limit > 10000) AND o.total > 500";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM orders o WHERE o.customer_id IN " +
                      "(SELECT c.id FROM customers c WHERE c.region = " + SqlParser.REDACTED_STRING + " " +
                      "AND c.credit_limit > " + SqlParser.REDACTED_NUMBER + ") AND o.total > " + SqlParser.REDACTED_NUMBER;

    assertFalse(result.contains("APAC"));
    assertFalse(result.contains("10000"));
    assertFalse(result.contains("500"));
    assertEquals(expected, result);
  }

  @Test
  public void testMultipleJoinsWithConditions() {
    String sql = "SELECT u.name, o.total, p.name FROM users u " +
                 "INNER JOIN orders o ON u.id = o.user_id " +
                 "LEFT JOIN products p ON o.product_id = p.id " +
                 "WHERE u.status = 'premium' AND o.total > 1000.00 AND p.category = 'electronics'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT u.name, o.total, p.name FROM users u " +
                      "INNER JOIN orders o ON u.id = o.user_id " +
                      "LEFT JOIN products p ON o.product_id = p.id " +
                      "WHERE u.status = " + SqlParser.REDACTED_STRING + " AND o.total > " + SqlParser.REDACTED_NUMBER
                      + " AND p.category = " + SqlParser.REDACTED_STRING;

    assertFalse(result.contains("premium"));
    assertFalse(result.contains("1000.00"));
    assertFalse(result.contains("electronics"));
    assertEquals(expected, result);
  }

  @Test
  public void testUnionWithLiterals() {
    String sql = "SELECT 'Customer' AS type, name FROM customers WHERE id = 1 " +
                 "UNION ALL " +
                 "SELECT 'Supplier' AS type, name FROM suppliers WHERE id = 2";
    String expected = "SELECT " + SqlParser.REDACTED_STRING + " AS type, name FROM customers WHERE id = "
        + SqlParser.REDACTED_NUMBER + " "
        + "UNION ALL "
        + "SELECT " + SqlParser.REDACTED_STRING + " AS type, name FROM suppliers WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    String result = SqlParser.redactSensitiveData(sql);

    assertEquals(expected, result);
  }

  @Test
  public void testGroupByHaving() {
    String sql = "SELECT category, COUNT(*) as cnt, AVG(price) as avg_price " +
                 "FROM products WHERE status = 'active' " +
                 "GROUP BY category HAVING AVG(price) > 50.00 AND COUNT(*) >= 10";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price " +
                      "FROM products WHERE status = " + SqlParser.REDACTED_STRING + " " +
                      "GROUP BY category HAVING AVG(price) > " + SqlParser.REDACTED_NUMBER
                      + " AND COUNT(*) >= " + SqlParser.REDACTED_NUMBER;

    assertFalse(result.contains("active"));
    assertFalse(result.contains("50.00"));
    assertEquals(expected, result);
  }

  @Test
  public void testExistsSubquery() {
    String sql = "SELECT * FROM customers c WHERE EXISTS " +
                 "(SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.total > 5000)";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM customers c WHERE EXISTS " +
                      "(SELECT " + SqlParser.REDACTED_NUMBER + " FROM orders o WHERE o.customer_id = c.id "
                      + "AND o.total > " + SqlParser.REDACTED_NUMBER + ")";

    assertFalse(result.contains("5000"));
    assertEquals(expected, result);
  }

  @Test
  public void testNotInWithStrings() {
    String sql = "SELECT * FROM products WHERE category NOT IN ('Obsolete', 'Discontinued', 'Archived')";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM products WHERE category NOT IN ("
        + SqlParser.REDACTED_STRING + ", " + SqlParser.REDACTED_STRING + ", " + SqlParser.REDACTED_STRING + ")";

    assertFalse(result.contains("Obsolete"));
    assertFalse(result.contains("Discontinued"));
    assertFalse(result.contains("Archived"));
    assertEquals(expected, result);
  }

  @Test
  public void testLikePatterns() {
    String sql = "SELECT * FROM users WHERE email LIKE '%@gmail.com' AND name LIKE 'John%'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM users WHERE email LIKE " + SqlParser.REDACTED_STRING
        + " AND name LIKE " + SqlParser.REDACTED_STRING;

    assertFalse(result.contains("@gmail.com"));
    assertFalse(result.contains("John"));
    assertEquals(expected, result);
  }

  // ========================================
  // Multiple-Databases Complex Query Tests
  // ========================================

  @Test
  public void testComplexMultiDatabaseWindowFunction() {
    String sql = "SELECT department, employee_name, salary, "
        + "RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank, "
        + "AVG(salary) OVER (PARTITION BY department) AS avg_dept_salary "
        + "FROM employees "
        + "WHERE hire_date > '2020-01-01' AND status = 'ACTIVE'";
    String expected = "SELECT department, employee_name, salary, "
        + "RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank, "
        + "AVG(salary) OVER (PARTITION BY department ) AS avg_dept_salary "
        + "FROM employees "
        + "WHERE hire_date > " + SqlParser.REDACTED_STRING + " AND status = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testComplexNestedSubqueriesValue() {
    String sql = "SELECT * FROM customers c WHERE c.total_orders > "
        + "(SELECT AVG(order_count) FROM "
        + "(SELECT customer_id, COUNT(*) AS order_count FROM orders "
        + "WHERE year = 2024 GROUP BY customer_id) subq) "
        + "AND c.status = 'Premium'";
    String expected = "SELECT * FROM customers c WHERE c.total_orders > "
        + "(SELECT AVG(order_count) FROM "
        + "(SELECT customer_id, COUNT(*) AS order_count FROM orders "
        + "WHERE year = " + SqlParser.REDACTED_NUMBER + " GROUP BY customer_id) subq) "
        + "AND c.status = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testComplexUnionQuery() {
    String sql = "SELECT id, name, 'customer' AS type FROM customers WHERE status = 'active' "
        + "UNION ALL "
        + "SELECT id, company_name, 'partner' FROM partners WHERE rating > 4.5";
    String expected = "SELECT id, name, " + SqlParser.REDACTED_STRING + " AS type FROM customers WHERE status = "
        + SqlParser.REDACTED_STRING + " "
        + "UNION ALL "
        + "SELECT id, company_name, " + SqlParser.REDACTED_STRING + " FROM partners WHERE rating > " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testComplexExistsClause() {
    String sql = "SELECT * FROM orders o WHERE EXISTS "
        + "(SELECT 1 FROM order_items oi WHERE oi.order_id = o.order_id "
        + "AND oi.price > 100.00) AND o.status = 'COMPLETED'";
    String expected = "SELECT * FROM orders o WHERE EXISTS "
        + "(SELECT " + SqlParser.REDACTED_NUMBER + " FROM order_items oi WHERE oi.order_id = o.order_id "
        + "AND oi.price > " + SqlParser.REDACTED_NUMBER + ") AND o.status = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMultipleJoinsWithConditionsValue() {
    String sql = "SELECT c.name, o.order_date, p.product_name, oi.quantity "
        + "FROM customers c "
        + "INNER JOIN orders o ON c.customer_id = o.customer_id AND o.total > 500 "
        + "INNER JOIN order_items oi ON o.order_id = oi.order_id "
        + "INNER JOIN products p ON oi.product_id = p.product_id "
        + "WHERE c.city = 'New York' AND o.year = 2024";
    String expected = "SELECT c.name, o.order_date, p.product_name, oi.quantity "
        + "FROM customers c "
        + "INNER JOIN orders o ON c.customer_id = o.customer_id AND o.total > " + SqlParser.REDACTED_NUMBER + " "
        + "INNER JOIN order_items oi ON o.order_id = oi.order_id "
        + "INNER JOIN products p ON oi.product_id = p.product_id "
        + "WHERE c.city = " + SqlParser.REDACTED_STRING + " AND o.year = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testBetweenClause() {
    String sql = "SELECT * FROM transactions WHERE amount BETWEEN 100.00 AND 500.00 "
        + "AND transaction_date BETWEEN '2024-01-01' AND '2024-12-31'";
    String expected = "SELECT * FROM transactions WHERE amount BETWEEN " + SqlParser.REDACTED_NUMBER
        + " AND " + SqlParser.REDACTED_NUMBER
        + " AND transaction_date BETWEEN " + SqlParser.REDACTED_STRING + " AND " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testInClauseWithStrings() {
    String sql = "SELECT * FROM orders WHERE status IN ('PENDING', 'PROCESSING', 'SHIPPED')";
    String expected = "SELECT * FROM orders WHERE status IN (" + SqlParser.REDACTED_STRING
        + ", " + SqlParser.REDACTED_STRING + ", " + SqlParser.REDACTED_STRING + ")";

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testLikePatternMatching() {
    String sql = "SELECT * FROM customers WHERE name LIKE '%Smith%' AND email LIKE 'john%@example.com'";
    String expected = "SELECT * FROM customers WHERE name LIKE " + SqlParser.REDACTED_STRING
        + " AND email LIKE " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  // ========================================
  // Oracle Database Specific Tests
  // ========================================

  @Test
  public void testOracleSelectFromDual() {
    String sql = "SELECT 'HELLO' FROM DUAL";
    String expected = "SELECT " + SqlParser.REDACTED_STRING + " FROM DUAL";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleRownumValue() {
    String sql = "SELECT * FROM employees WHERE ROWNUM <= 10 AND salary > 50000";
    String expected = "SELECT * FROM employees WHERE ROWNUM <= " + SqlParser.REDACTED_NUMBER
        + " AND salary > " + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleNvlFunctionValue() {
    String sql = "SELECT NVL(commission_pct, 0) FROM employees WHERE employee_id = 100";
    String expected = "SELECT NVL(commission_pct, " + SqlParser.REDACTED_NUMBER
        + ") FROM employees WHERE employee_id = " + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleDecodeFunction() {
    String sql = "SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') FROM users WHERE id = 1";
    String expected = "SELECT DECODE(status, " + SqlParser.REDACTED_STRING + ", " + SqlParser.REDACTED_STRING
        + ", " + SqlParser.REDACTED_STRING + ", " + SqlParser.REDACTED_STRING + ", " + SqlParser.REDACTED_STRING
        + ") FROM users WHERE id = " + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleMergeStatementValue() {
    String sql = "MERGE INTO target t USING source s ON (t.id = s.id) "
        + "WHEN MATCHED THEN UPDATE SET t.value = 'updated' "
        + "WHEN NOT MATCHED THEN INSERT VALUES (1, 'new')";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse(result.contains("'updated'"));
    assertFalse(result.contains("'new'"));
    assertFalse(result.contains("(1,"));
    assertTrue(result.contains(SqlParser.REDACTED_STRING));
    assertTrue(result.contains(SqlParser.REDACTED_NUMBER));
  }

  @Test
  public void testOracleConnectByPrior() {
    String sql =
        "SELECT employee_id, manager_id, level FROM employees "
            + "WHERE salary > 50000 START WITH manager_id IS NULL "
            + "CONNECT BY PRIOR employee_id = manager_id";
    String result = SqlParser.redactSensitiveData(sql);

    String expected =
        "SELECT employee_id, manager_id, level FROM employees "
            + "WHERE salary > "
            + SqlParser.REDACTED_NUMBER
            + " START WITH manager_id IS NULL "
            + "CONNECT BY PRIOR employee_id = manager_id";
    assertEquals(expected, result);
  }

  @Test
  public void testOracleToDateFunctionValue() {
    String sql = "SELECT * FROM orders WHERE order_date = TO_DATE('2024-01-15', 'YYYY-MM-DD')";
    String expected = "SELECT * FROM orders WHERE order_date = TO_DATE(" + SqlParser.REDACTED_STRING
        + ", " + SqlParser.REDACTED_STRING + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleRownum() {
    String sql = "SELECT * FROM employees WHERE ROWNUM <= 10 AND salary > 50000";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM employees WHERE ROWNUM <= " + SqlParser.REDACTED_NUMBER
        + " AND salary > " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, result);
  }

  @Test
  public void testOracleToCharFunction() {
    String sql = "SELECT TO_CHAR(hire_date, 'DD-MON-YYYY') FROM employees WHERE employee_id = 100";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT TO_CHAR(hire_date, " + SqlParser.REDACTED_STRING
        + ") FROM employees WHERE employee_id = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, result);
  }

  @Test
  public void testOracleBetweenWithDates() {
    String sql = "SELECT * FROM sales WHERE sale_date BETWEEN TO_DATE('2023-01-01', 'YYYY-MM-DD') " +
                 "AND TO_DATE('2023-12-31', 'YYYY-MM-DD')";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM sales WHERE sale_date BETWEEN TO_DATE(" + SqlParser.REDACTED_STRING
        + ", " + SqlParser.REDACTED_STRING + ") AND TO_DATE(" + SqlParser.REDACTED_STRING + ", " + SqlParser.REDACTED_STRING + ")";

    assertFalse(result.contains("2023-01-01"));
    assertFalse(result.contains("2023-12-31"));
    assertEquals(expected, result);
  }

  @Test
  public void testOracleSubstrInstr() {
    String sql = "SELECT SUBSTR(name, 1, 10) FROM employees WHERE INSTR(email, '@') > 0";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT SUBSTR(name, " + SqlParser.REDACTED_NUMBER + ", " + SqlParser.REDACTED_NUMBER
        + ") FROM employees WHERE INSTR(email, " + SqlParser.REDACTED_STRING + ") > " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, result);
  }

  // ========================================
  // PostgreSQL Database Specific Tests
  // ========================================
  @Test
  public void testPostgresArrayLiteral() {
    String sql = "SELECT * FROM products WHERE tags = ARRAY['electronics', 'sale']";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse(result.contains("'electronics'"));
    assertFalse(result.contains("'sale'"));
    assertTrue(result.contains(SqlParser.REDACTED_STRING));
  }

  @Test
  public void testPostgresJsonOperator() {
    String sql = "SELECT data->>'name' FROM users WHERE data->>'status' = 'active'";
    String expected = "SELECT data->>'name' FROM users WHERE data->>'status' = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresRegexMatch() {
    String sql = "SELECT * FROM products WHERE name ~ 'pattern' AND price > 99.99";
    String expected = "SELECT * FROM products WHERE name ~ " + SqlParser.REDACTED_STRING
        + " AND price > " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresWindowFunctionValue() {
    String sql = "SELECT employee_id, salary, "
        + "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank "
        + "FROM employees WHERE hire_date > '2020-01-01'";
    String expected = "SELECT employee_id, salary, "
        + "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank "
        + "FROM employees WHERE hire_date > " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresILikeOperator() {
    String sql = "SELECT * FROM customers WHERE name ILIKE '%smith%' AND age >= 30";
    String expected = "SELECT * FROM customers WHERE name ILIKE " + SqlParser.REDACTED_STRING
        + " AND age >= " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresDistinctOn() {
    String sql = "SELECT DISTINCT ON (department) id, name, salary FROM employees WHERE salary > 60000";
    String result = SqlParser.redactSensitiveData(sql);
    assertTrue(result.contains(SqlParser.REDACTED_NUMBER));
    assertFalse(result.contains("60000"));
  }

  @Test
  public void testPostgresLimitOffset() {
    String sql = "SELECT * FROM orders WHERE status = 'PENDING' ORDER BY created_at LIMIT 20 OFFSET 40";
    String result = SqlParser.redactSensitiveData(sql);
    assertEquals("SELECT * FROM orders WHERE status = " + SqlParser.REDACTED_STRING
        + " ORDER BY created_at LIMIT " + SqlParser.REDACTED_NUMBER + " OFFSET " + SqlParser.REDACTED_NUMBER, result);
  }

  @Test
  public void testPostgresCoalesce() {
    String sql = "SELECT COALESCE(nickname, first_name, 'Unknown') FROM users WHERE id = 42";
    String result = SqlParser.redactSensitiveData(sql);
    assertEquals("SELECT COALESCE(nickname, first_name, " + SqlParser.REDACTED_STRING + ") FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER, result);
  }

  @Test
  public void testPostgresNullif() {
    String sql = "SELECT NULLIF(status, 'UNKNOWN') FROM records WHERE id = 100";
    String result = SqlParser.redactSensitiveData(sql);
    assertEquals("SELECT NULLIF(status, " + SqlParser.REDACTED_STRING + ") FROM records WHERE id = "
        + SqlParser.REDACTED_NUMBER, result);
  }

  @Test
  public void testPostgresWindowFunction() {
    String sql = "SELECT name, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) " +
                 "FROM employees WHERE salary > 50000";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT name, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) " +
                      "FROM employees WHERE salary > " + SqlParser.REDACTED_NUMBER;

    assertFalse(result.contains("50000"));
    assertEquals(expected, result);
  }

  // ========================================
  // SQL Server Specific Tests
  // ========================================

  @Test
  public void testSqlServerTopClause() {
    String sql = "SELECT TOP 10 * FROM customers WHERE city = 'Seattle' ORDER BY id";
    String expected = "SELECT TOP 10"
        + " * FROM customers WHERE city = " + SqlParser.REDACTED_STRING + " ORDER BY id";

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSqlServerTopPercentValue() {
    String sql = "SELECT TOP 25 PERCENT * FROM orders WHERE total > 1000";
    String expected = "SELECT TOP 25 PERCENT * FROM orders WHERE total > " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSqlServerOffsetFetchValue() {
    String sql = "SELECT * FROM products ORDER BY product_id OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY";
    String expected = "SELECT * FROM products ORDER BY product_id OFFSET " + SqlParser.REDACTED_NUMBER
        + " ROWS FETCH NEXT " + SqlParser.REDACTED_NUMBER + " ROWS ONLY";

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSqlServerCrossApply() {
    String sql = "SELECT c.customer_id, o.order_id FROM customers c "
        + "CROSS APPLY (SELECT TOP 5 * FROM orders WHERE customer_id = c.customer_id "
        + "AND amount > 100) o";
    String expected = "SELECT c.customer_id, o.order_id FROM customers c "
        + "CROSS APPLY (SELECT TOP 5 * FROM orders WHERE customer_id = c.customer_id "
        + "AND amount > " + SqlParser.REDACTED_NUMBER + ") o";

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSqlServerIIF() {
    String sql = "SELECT IIF(price > 100, 'Expensive', 'Cheap') FROM products WHERE category = 'Electronics'";
    String expected = "SELECT IIF(price > " + SqlParser.REDACTED_NUMBER + ", " + SqlParser.REDACTED_STRING
        + ", " + SqlParser.REDACTED_STRING + ") FROM products WHERE category = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSqlServerFormat() {
    String sql = "SELECT FORMAT(order_date, 'yyyy-MM-dd') FROM orders WHERE total > 500.00";
    String expected = "SELECT FORMAT(order_date, " + SqlParser.REDACTED_STRING
        + ") FROM orders WHERE total > " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSqlServerStringConcat() {
    String sql = "SELECT CONCAT(first_name, ' ', last_name) FROM employees WHERE department = 'Sales'";
    String expected = "SELECT CONCAT(first_name, " + SqlParser.REDACTED_STRING
        + ", last_name) FROM employees WHERE department = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSqlServerWithNolock() {
    String sql = "SELECT * FROM products WITH (NOLOCK) WHERE category_id = 5 AND price < 99.99";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM products WITH (NOLOCK) WHERE category_id = "
        + SqlParser.REDACTED_NUMBER + " AND price < " + SqlParser.REDACTED_NUMBER;

    assertFalse(result.contains("99.99"));
    assertEquals(expected, result);
  }

  @Test
  public void testSqlServerIsNull() {
    String sql = "SELECT ISNULL(middle_name, 'N/A') FROM employees WHERE employee_id = 1001";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT ISNULL(middle_name, " + SqlParser.REDACTED_STRING
        + ") FROM employees WHERE employee_id = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, result);
  }

  @Test
  public void testSqlServerConvert() {
    String sql = "SELECT CONVERT(VARCHAR(10), hire_date, 101) FROM employees WHERE dept = 'IT'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT CONVERT(VARCHAR(0), hire_date, " + SqlParser.REDACTED_NUMBER
        + ") FROM employees WHERE dept = " + SqlParser.REDACTED_STRING;

    assertFalse(result.contains("101"));
    assertEquals(expected, result);
  }

  @Test
  public void testSqlServerCast() {
    String sql = "SELECT CAST(price AS DECIMAL(10, 2)) FROM products WHERE id IN (1, 2, 3)";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT CAST(price AS DECIMAL (10, 2)) FROM products WHERE id IN ("
        + SqlParser.REDACTED_NUMBER + ", " + SqlParser.REDACTED_NUMBER + ", " + SqlParser.REDACTED_NUMBER + ")";

    assertEquals(expected, result);
  }

  @Test
  public void testSqlServerDateDiff() {
    String sql = "SELECT * FROM subscriptions WHERE DATEDIFF(day, start_date, '2023-12-31') > 365";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM subscriptions WHERE DATEDIFF(day, start_date, "
        + SqlParser.REDACTED_STRING + ") > " + SqlParser.REDACTED_NUMBER;

    assertFalse(result.contains("2023-12-31"));
    assertFalse(result.contains("365"));
    assertEquals(expected, result);
  }

  @Test
  public void testSqlServerLen() {
    String sql = "SELECT * FROM messages WHERE LEN(content) > 1000 AND sender = 'admin'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM messages WHERE LEN(content) > "
        + SqlParser.REDACTED_NUMBER + " AND sender = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, result);
  }

  @Test
  public void testSqlServerCharIndex() {
    String sql = "SELECT CHARINDEX('@', email) FROM users WHERE id = 500";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT CHARINDEX(" + SqlParser.REDACTED_STRING
        + ", email) FROM users WHERE id = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, result);
  }

  // ========================================
  // IBM DB2 Database Specific Tests
  // ========================================

  @Test
  public void testDB2FetchFirstRows() {
    String sql = "SELECT * FROM employees WHERE salary > 80000 FETCH FIRST 10 ROWS ONLY";
    String expected = "SELECT * FROM employees WHERE salary > " + SqlParser.REDACTED_NUMBER
        + " FETCH FIRST " + SqlParser.REDACTED_NUMBER + " ROWS ONLY";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testDB2OptimizeFor() {
    String sql = "SELECT * FROM orders WHERE customer_id = 12345 OPTIMIZE FOR 100 ROWS";
    String expected = "SELECT * FROM orders WHERE customer_id = " + SqlParser.REDACTED_NUMBER
        + " OPTIMIZE FOR 100 ROWS";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testDB2ValueFunction() {
    String sql = "SELECT VALUE(commission, 0) FROM sales WHERE region = 'WEST' AND year = 2024";
    String expected = "SELECT VALUE(commission, " + SqlParser.REDACTED_NUMBER
        + ") FROM sales WHERE region = " + SqlParser.REDACTED_STRING + " AND year = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testDB2WithIsolation() {
    String sql = "SELECT * FROM accounts WHERE balance > 10000 WITH UR";
    String expected = "SELECT * FROM accounts WHERE balance > " + SqlParser.REDACTED_NUMBER + " WITH UR";

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testDB2CaseExpression() {
    String sql = "SELECT CASE WHEN salary > 100000 THEN 'High' "
        + "WHEN salary > 50000 THEN 'Medium' ELSE 'Low' END FROM employees WHERE dept = 'IT'";
    String expected = "SELECT CASE WHEN salary > " + SqlParser.REDACTED_NUMBER + " THEN " + SqlParser.REDACTED_STRING + " "
        + "WHEN salary > " + SqlParser.REDACTED_NUMBER + " THEN " + SqlParser.REDACTED_STRING
        + " ELSE " + SqlParser.REDACTED_STRING + " END FROM employees WHERE dept = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testDB2CoalesceFunction() {
    String sql = "SELECT COALESCE(phone, mobile, 'N/A') FROM contacts WHERE status = 'active'";
    String expected = "SELECT COALESCE(phone, mobile, " + SqlParser.REDACTED_STRING
        + ") FROM contacts WHERE status = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testDB2SelectFromSysibm() {
    String sql = "SELECT * FROM SYSIBM.SYSDUMMY1 WHERE 'test' = 'test'";
    String expected = "SELECT * FROM SYSIBM.SYSDUMMY1 WHERE " + SqlParser.REDACTED_STRING
        + " = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testDb2FetchFirst() {
    String sql = "SELECT * FROM employees WHERE salary > 75000 FETCH FIRST 50 ROWS ONLY";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM employees WHERE salary > "
        + SqlParser.REDACTED_NUMBER + " FETCH FIRST " + SqlParser.REDACTED_NUMBER + " ROWS ONLY";

    assertFalse(result.contains("75000"));
    assertFalse(result.contains("50"));
    assertEquals(expected, result);
  }

  @Test
  public void testDb2ConcatOperator() {
    String sql = "SELECT first_name || ' ' || last_name AS full_name FROM employees WHERE dept = 'HR'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT first_name || " + SqlParser.REDACTED_STRING
        + " || last_name AS full_name FROM employees WHERE dept = " + SqlParser.REDACTED_STRING;

    assertFalse(result.contains("HR"));
    assertEquals(expected, result);
  }

  @Test
  public void testDb2ValueFunction() {
    String sql = "SELECT VALUE(commission, 0) FROM sales WHERE rep_id = 999";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT VALUE(commission, " + SqlParser.REDACTED_NUMBER
        + ") FROM sales WHERE rep_id = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, result);
  }

  @Test
  public void testDb2SubstrFunction() {
    String sql = "SELECT SUBSTR(description, 1, 100) FROM products WHERE category = 'Electronics'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT SUBSTR(description, " + SqlParser.REDACTED_NUMBER + ", " + SqlParser.REDACTED_NUMBER
        + ") FROM products WHERE category = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, result);
  }

  @Test
  public void testDb2RowNumber() {
    String sql = "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees WHERE dept_id = 10";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees WHERE dept_id = "
        + SqlParser.REDACTED_NUMBER;

    assertFalse(result.contains(" 10"));
    assertEquals(expected, result);
  }

  @Test
  public void testDb2DateArithmetic() {
    String sql = "SELECT * FROM orders WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM orders WHERE order_date BETWEEN "
        + SqlParser.REDACTED_STRING + " AND " + SqlParser.REDACTED_STRING;

    assertFalse(result.contains("2023-01-01"));
    assertFalse(result.contains("2023-12-31"));
    assertEquals(expected, result);
  }

  @Test
  public void testDb2LeftFunction() {
    String sql = "SELECT LEFT(product_code, 3) FROM inventory WHERE quantity > 100";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT LEFT(product_code, " + SqlParser.REDACTED_NUMBER + ") FROM inventory WHERE quantity > "
        + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, result);
  }

  @Test
  public void testDb2ExceptClause() {
    String sql = "SELECT id FROM table1 WHERE status = 'active' EXCEPT SELECT id FROM table2 WHERE flag = 1";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT id FROM table1 WHERE status = " + SqlParser.REDACTED_STRING
        + " EXCEPT SELECT id FROM table2 WHERE flag = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, result);
  }

  // ========================================
  // MySQL Database Functions Tests
  // ========================================

  @Test
  public void testMysqlLimit() {
    String sql = "SELECT * FROM users WHERE status = 'active' LIMIT 25";
    String result = SqlParser.redactSensitiveData(sql);

    assertEquals("SELECT * FROM users WHERE status = " + SqlParser.REDACTED_STRING + " LIMIT " + SqlParser.REDACTED_NUMBER, result);
  }

  @Test
  public void testMySQLLimitOffset() {
    String sql = "SELECT * FROM products ORDER BY price DESC LIMIT 20 OFFSET 40";
    String expected = "SELECT * FROM products ORDER BY price DESC LIMIT " + SqlParser.REDACTED_NUMBER
        + " OFFSET " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLRegexp() {
    String sql = "SELECT * FROM customers WHERE email REGEXP '^[a-z]+@example\\.com$' AND status = 1";
    String expected = "SELECT * FROM customers WHERE email REGEXP " + SqlParser.REDACTED_STRING
        + " AND status = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLCastFunction() {
    String sql = "SELECT CAST('2024-01-15' AS DATE) FROM orders WHERE id = 123";
    String expected = "SELECT CAST(" + SqlParser.REDACTED_STRING
        + " AS DATE) FROM orders WHERE id = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLConcatWs() {
    String sql = "SELECT CONCAT_WS(',', first_name, last_name, 'extra') FROM users WHERE id = 456";
    String expected = "SELECT CONCAT_WS(" + SqlParser.REDACTED_STRING + ", first_name, last_name, "
        + SqlParser.REDACTED_STRING + ") FROM users WHERE id = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLDateAdd() {
    String sql = "SELECT * FROM orders WHERE order_date > DATE_ADD('2024-01-01', INTERVAL 30 DAY)";
    String expected = "SELECT * FROM orders WHERE order_date > DATE_ADD(" + SqlParser.REDACTED_STRING
        + ", INTERVAL " + SqlParser.REDACTED_NUMBER + " DAY)";

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMysqlIfNull() {
    String sql = "SELECT IFNULL(nickname, 'Guest') FROM users WHERE id = 42";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT IFNULL(nickname, " + SqlParser.REDACTED_STRING
        + ") FROM users WHERE id = " + SqlParser.REDACTED_NUMBER;

    assertEquals(expected, result);
  }

  @Test
  public void testMysqlIfFunction() {
    String sql = "SELECT IF(score >= 60, 'Pass', 'Fail') FROM exams WHERE student_id = 12345";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT IF(score >= " + SqlParser.REDACTED_NUMBER + ", " + SqlParser.REDACTED_STRING
        + ", " + SqlParser.REDACTED_STRING + ") FROM exams WHERE student_id = " + SqlParser.REDACTED_NUMBER;

    assertFalse(result.contains("60"));
    assertFalse(result.contains("Pass"));
    assertFalse(result.contains("Fail"));
    assertEquals(expected, result);
  }

  @Test
  public void testMysqlDateFormat() {
    String sql = "SELECT DATE_FORMAT(created_at, '%Y-%m-%d') FROM logs WHERE user_id = 999";
    String result = SqlParser.redactSensitiveData(sql);
    assertEquals("SELECT DATE_FORMAT(created_at, " + SqlParser.REDACTED_STRING + ") FROM logs WHERE user_id = "
        + SqlParser.REDACTED_NUMBER, result);
  }

  @Test
  public void testMysqlSubstringIndex() {
    String sql = "SELECT SUBSTRING_INDEX(email, '@', 1) FROM users WHERE id = 100";
    String result = SqlParser.redactSensitiveData(sql);
    assertEquals("SELECT SUBSTRING_INDEX(email, " + SqlParser.REDACTED_STRING + ", " + SqlParser.REDACTED_NUMBER
        + ") FROM users WHERE id = " + SqlParser.REDACTED_NUMBER, result);
  }

  @Test
  public void testMysqlCaseInsensitiveCollation() {
    String sql = "SELECT * FROM products WHERE name = 'iPhone' COLLATE utf8mb4_general_ci";
    String result = SqlParser.redactSensitiveData(sql);

    assertEquals("SELECT * FROM products WHERE name = " + SqlParser.REDACTED_STRING + " COLLATE utf8mb4_general_ci", result);
  }

  @Test
  public void testMysqlBetween() {
    String sql = "SELECT * FROM orders WHERE amount BETWEEN 100.00 AND 500.00 AND status = 'completed'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM orders WHERE amount BETWEEN " + SqlParser.REDACTED_NUMBER
        + " AND " + SqlParser.REDACTED_NUMBER + " AND status = " + SqlParser.REDACTED_STRING;

    assertFalse(result.contains("100.00"));
    assertFalse(result.contains("500.00"));
    assertFalse(result.contains("completed"));
    assertEquals(expected, result);
  }
}
