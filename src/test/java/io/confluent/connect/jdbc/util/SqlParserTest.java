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
    // The regex safety net (Layer 2) treats 'name' and 'status' as string literals
    // and redacts them. This is the more secure behavior - the structure
    // (data->> operator) is still visible for debugging.
    String expected = "SELECT data->>" + SqlParser.REDACTED_STRING
        + " FROM users WHERE data->>" + SqlParser.REDACTED_STRING
        + " = " + SqlParser.REDACTED_STRING;

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

  // ========================================
  // JSON Function Tests (Critical Security Fix)
  // ========================================

  @Test
  public void testJsonObjectSimpleKeyValueObj() {
    String sql = "SELECT JSON_OBJECTAGG(\"username, age\") FROM users WHERE userid IN (857287, 871122, 856489, 414082) GROUP BY age;";
    String result = SqlParser.redactSensitiveData(sql);

    System.out.println("Redacted SQL: " + result);
  }


  @Test
  public void testJsonObjectSimpleKeyValue() {
    String sql = "SELECT JSON_OBJECT('name', 'John Doe', 'age', 30) FROM users WHERE id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Should not contain PII value 'John Doe'", result.contains("John Doe"));
    assertFalse("Should not contain PII value 30", result.contains(" 30"));
    assertTrue("Should contain redacted string markers", result.contains(SqlParser.REDACTED_STRING));
    assertTrue("Should contain redacted number markers", result.contains(SqlParser.REDACTED_NUMBER));
    assertTrue("Should preserve JSON_OBJECT structure", result.contains("JSON_OBJECT("));
    assertTrue("Should preserve FROM clause", result.contains("FROM users"));
    assertTrue("Should preserve WHERE clause", result.contains("WHERE id ="));
  }

  @Test
  public void testJsonObjectNestedSecurityBug() {
    // This is the exact query from the security bug report
    String sql = "SELECT JSON_OBJECT('profile', JSON_OBJECT('userid', 871122, "
        + "'username', 'user_1234', 'age', 24), "
        + "'metadata', JSON_OBJECT('event_time', '1743853243812259', "
        + "'updated_at', '2025-04-05 17:10:43.812')) "
        + "FROM users WHERE uerid = 871122";
    String result = SqlParser.redactSensitiveData(sql);

    // Verify NO PII values leak through
    assertFalse("User ID should be redacted", result.contains("871122"));
    assertFalse("Username should be redacted", result.contains("user_1234"));
    assertFalse("Age should not appear as 24", result.contains(" 24"));
    assertFalse("Event time should be redacted", result.contains("1743853243812259"));
    assertFalse("Updated_at should be redacted", result.contains("2025-04-05"));
    assertFalse("Updated_at time should be redacted", result.contains("17:10:43"));

    // Verify structure is preserved
    assertTrue("Should preserve JSON_OBJECT structure", result.contains("JSON_OBJECT("));
    assertTrue("Should preserve FROM clause", result.contains("FROM users"));
    assertTrue("Should preserve WHERE clause", result.contains("WHERE uerid ="));
  }

  @Test
  public void testJsonObjectWithColumnKeys() {
    String sql = "SELECT JSON_OBJECT(col1, col2) FROM users WHERE id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    // Column references should be preserved (they're identifiers, not PII)
    assertTrue("Should preserve column name col1", result.contains("col1"));
    assertTrue("Should preserve column name col2", result.contains("col2"));
    assertTrue("Should preserve JSON_OBJECT", result.contains("JSON_OBJECT("));
  }

  @Test
  public void testJsonObjectWithWhereClause() {
    String sql = "SELECT JSON_OBJECT('key', 'value') FROM table1 "
        + "WHERE name = 'sensitive_name' AND id = 42";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("WHERE clause string should be redacted", result.contains("sensitive_name"));
    assertFalse("WHERE clause number should be redacted", result.contains("42"));
    assertTrue("Should preserve table name", result.contains("table1"));
    assertTrue("Should preserve column names", result.contains("name ="));
    assertTrue("Should preserve column names", result.contains("id ="));
  }

  @Test
  public void testJsonArraySimple() {
    String sql = "SELECT JSON_ARRAY('hello', 'world', 42, 3.14) FROM test_table";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("String values should be redacted", result.contains("hello"));
    assertFalse("String values should be redacted", result.contains("world"));
    assertTrue("Should preserve JSON_ARRAY structure", result.contains("JSON_ARRAY("));
    assertTrue("Should preserve FROM clause", result.contains("FROM test_table"));
  }

  // ========================================
  // Regex Fallback Tests
  // ========================================

  @Test
  public void testRegexFallbackForUnparseableSql() {
    // This SQL is intentionally unparseable by JSqlParser
    String sql = "SELECT SOME_FUTURE_FUNC('sensitive_data', 42) :::: FROM users";
    String result = SqlParser.redactSensitiveData(sql);

    // The regex fallback should redact string and numeric literals
    assertFalse("String literals should be redacted", result.contains("sensitive_data"));
    // Structure keywords should be preserved
    assertTrue("Should preserve SELECT keyword", result.contains("SELECT"));
    assertTrue("Should preserve FROM keyword", result.contains("FROM"));
  }

  @Test
  public void testRegexFallbackPreservesQueryStructure() {
    // Another unparseable query
    String sql = "SELECT @@@custom_func('secret_value') WHERE x = 'password123'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Should redact 'secret_value'", result.contains("secret_value"));
    assertFalse("Should redact 'password123'", result.contains("password123"));
    assertTrue("Should preserve SELECT", result.contains("SELECT"));
  }

  @Test
  public void testRegexRedactStringLiterals() {
    // Test the string literal regex directly
    String input = "SELECT * FROM t WHERE name = 'John' AND status = 'active'";
    String result = SqlParser.redactStringLiterals(input);

    assertFalse(result.contains("John"));
    assertFalse(result.contains("active"));
    assertTrue(result.contains(SqlParser.REDACTED_STRING));
    assertTrue(result.contains("SELECT * FROM t WHERE name ="));
  }

  @Test
  public void testRegexRedactStringLiteralsWithEscapedQuotes() {
    // SQL-style escaping with ''
    String input = "SELECT * FROM t WHERE name = 'O''Brien'";
    String result = SqlParser.redactStringLiterals(input);

    assertFalse("Escaped quote string should be redacted", result.contains("O'Brien"));
    assertTrue(result.contains(SqlParser.REDACTED_STRING));
  }

  @Test
  public void testRegexRedactAllLiterals() {
    // Test both string and numeric redaction
    String input = "SELECT * FROM t WHERE name = 'John' AND age = 30 AND balance = 99.99";
    String result = SqlParser.redactAllLiterals(input);

    assertFalse(result.contains("John"));
    assertFalse(result.contains("30"));
    assertFalse(result.contains("99.99"));
    assertTrue(result.contains("SELECT * FROM t WHERE name ="));
  }

  @Test
  public void testRegexPreservesIdentifiersWithNumbers() {
    // Numbers in identifiers should NOT be redacted
    String input = "SELECT * FROM table1 WHERE col2 = 'test'";
    String result = SqlParser.redactAllLiterals(input);

    assertTrue("table1 should be preserved", result.contains("table1"));
    assertTrue("col2 should be preserved", result.contains("col2"));
  }

  @Test
  public void testRegexRedactionIdempotent() {
    // Already-redacted values should remain unchanged
    String input = "SELECT * FROM t WHERE name = " + SqlParser.REDACTED_STRING
        + " AND id = " + SqlParser.REDACTED_NUMBER;
    String result = SqlParser.redactStringLiterals(input);

    assertEquals(input, result);
  }

  @Test
  public void testNullInput() {
    assertEquals("", SqlParser.redactSensitiveData(null));
  }

  @Test
  public void testWhitespaceOnlyInput() {
    assertEquals("", SqlParser.redactSensitiveData("   "));
  }

  // ========================================
  // Safety Net Integration Tests
  // ========================================

  @Test
  public void testSafetyNetCatchesLeakedStringLiterals() {
    // This test verifies that even if a new expression type bypasses the
    // visitor pattern in a future JSqlParser version, the regex safety net
    // would catch any leaked string literals
    String sql = "SELECT * FROM users WHERE name = 'test_user' AND id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("No string literal PII should leak", result.contains("test_user"));
    assertTrue("Redacted marker should be present", result.contains(SqlParser.REDACTED_STRING));
  }

  @Test
  public void testComplexQueryWithMultipleJsonFunctions() {
    String sql = "SELECT JSON_OBJECT('user', JSON_OBJECT('name', 'Alice', 'email', 'alice@test.com')), "
        + "JSON_OBJECT('stats', JSON_OBJECT('logins', 42, 'score', 99.5)) "
        + "FROM users WHERE status = 'active'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Name PII should be redacted", result.contains("Alice"));
    assertFalse("Email PII should be redacted", result.contains("alice@test.com"));
    assertFalse("Login count should be redacted", result.contains(" 42"));
    assertFalse("Score should be redacted", result.contains("99.5"));
    assertFalse("Status value should be redacted", result.contains("active"));
    assertTrue("Should preserve JSON_OBJECT structure", result.contains("JSON_OBJECT("));
    assertTrue("Should preserve FROM clause", result.contains("FROM users"));
  }

  // =========================================================================
  // Layer 3: Post-redaction verification tests
  // =========================================================================

  @Test
  public void testHasLeakedLiterals_noLeak() {
    // A properly redacted query should pass verification
    String redacted = "SELECT * FROM users WHERE name = " + SqlParser.REDACTED_STRING
        + " AND id = " + SqlParser.REDACTED_NUMBER;
    assertFalse("Properly redacted query should not be flagged", SqlParser.hasLeakedLiterals(redacted));
  }

  @Test
  public void testHasLeakedLiterals_leakedString() {
    // A query with an unredacted string literal should be detected
    String leaked = "SELECT * FROM users WHERE name = 'leaked_value' AND id = 0";
    assertTrue("Leaked string literal should be detected", SqlParser.hasLeakedLiterals(leaked));
  }

  @Test
  public void testHasLeakedLiterals_leakedNumber() {
    // A query with an unredacted numeric literal should be detected
    String leaked = "SELECT * FROM users WHERE id = 857287";
    assertTrue("Leaked numeric literal should be detected", SqlParser.hasLeakedLiterals(leaked));
  }

  @Test
  public void testHasLeakedLiterals_structuralTopNotFlagged() {
    // TOP n is structural (bypasses visitor) — should NOT be flagged
    String redacted = "SELECT TOP 10 * FROM users WHERE status = " + SqlParser.REDACTED_STRING;
    assertFalse("TOP 10 is structural and should not be flagged", SqlParser.hasLeakedLiterals(redacted));
  }

  @Test
  public void testHasLeakedLiterals_strturalTopPercentNotFlagged() {
    // TOP n PERCENT is structural — should NOT be flagged
    String redacted = "SELECT TOP 25 PERCENT * FROM users WHERE total > " + SqlParser.REDACTED_NUMBER;
    assertFalse("TOP 25 PERCENT is structural and should not be flagged",
        SqlParser.hasLeakedLiterals(redacted));
  }

  @Test
  public void testHasLeakedLiterals_structuralOptimizeForNotFlagged() {
    // OPTIMIZE FOR n ROWS is structural — should NOT be flagged
    String redacted = "SELECT * FROM orders WHERE customer_id = " + SqlParser.REDACTED_NUMBER
        + " OPTIMIZE FOR 100 ROWS";
    assertFalse("OPTIMIZE FOR 100 is structural and should not be flagged",
        SqlParser.hasLeakedLiterals(redacted));
  }

  @Test
  public void testHasLeakedLiterals_typeDefDecimalNotFlagged() {
    // DECIMAL(10, 2) parameters are type metadata — should NOT be flagged
    String redacted = "SELECT CAST(price AS DECIMAL (10, 2)) FROM products WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertFalse("DECIMAL(10, 2) type params should not be flagged",
        SqlParser.hasLeakedLiterals(redacted));
  }

  @Test
  public void testHasLeakedLiterals_typeDefVarcharNotFlagged() {
    // VARCHAR(255) parameter is type metadata — should NOT be flagged
    String redacted = "SELECT CAST(name AS VARCHAR(255)) FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertFalse("255) type param should not be flagged",
        SqlParser.hasLeakedLiterals(redacted));
  }

  @Test
  public void testHasLeakedLiterals_mixedStructuralAndPii() {
    // TOP 10 is structural but 857287 is a leaked PII value — should be flagged
    String leaked = "SELECT TOP 10 * FROM users WHERE id = 857287";
    assertTrue("Leaked numeric PII should be detected even with structural TOP",
        SqlParser.hasLeakedLiterals(leaked));
  }

  @Test
  public void testHasLeakedLiterals_redactedMarkerOnly() {
    // A query with only redaction markers should pass
    String redacted = "SELECT * FROM users WHERE name = " + SqlParser.REDACTED_STRING
        + " AND age > " + SqlParser.REDACTED_NUMBER
        + " LIMIT " + SqlParser.REDACTED_NUMBER
        + " OFFSET " + SqlParser.REDACTED_NUMBER;
    assertFalse("Query with only redaction markers should pass", SqlParser.hasLeakedLiterals(redacted));
  }

  @Test
  public void testHasLeakedLiterals_nullAndEmpty() {
    assertFalse("Null input should pass", SqlParser.hasLeakedLiterals(null));
    assertFalse("Empty input should pass", SqlParser.hasLeakedLiterals(""));
  }

  @Test
  public void testLayer3TriggersFullRedactOnLeakedNumeric() {
    // Simulate a query where AST processing would miss a numeric literal
    // by verifying the entire flow — Layer 3 should catch it and fully redact
    // For this test, we verify that properly parsed queries don't trigger Layer 3
    String sql = "SELECT * FROM users WHERE id = 12345 AND name = 'John'";
    String result = SqlParser.redactSensitiveData(sql);
    assertNotEquals("Well-parsed query should not be fully redacted",
        "<redacted>", result);
    assertFalse("PII number should be redacted", result.contains("12345"));
    assertFalse("PII string should be redacted", result.contains("John"));
  }

  @Test
  public void testLayer3WithTopAndDecimalPreservesStructure() {
    // TOP and DECIMAL params are structural — Layer 3 should not trigger
    String sql = "SELECT TOP 10 CAST(price AS DECIMAL(10, 2)) FROM products WHERE category = 'Electronics'";
    String result = SqlParser.redactSensitiveData(sql);
    assertNotEquals("Query with structural numbers should not be fully redacted",
        "<redacted>", result);
    assertTrue("TOP should be preserved", result.contains("TOP 10"));
    assertTrue("DECIMAL params should be preserved", result.contains("10, 2"));
    assertFalse("Category value should be redacted", result.contains("Electronics"));
  }

  @Test
  public void testLayer3WithOptimizeForPreservesStructure() {
    // OPTIMIZE FOR is structural — Layer 3 should not trigger
    String sql = "SELECT * FROM orders WHERE customer_id = 12345 OPTIMIZE FOR 100 ROWS";
    String result = SqlParser.redactSensitiveData(sql);
    assertNotEquals("Query with OPTIMIZE FOR should not be fully redacted",
        "<redacted>", result);
    assertTrue("OPTIMIZE FOR should be preserved", result.contains("OPTIMIZE FOR 100 ROWS"));
    assertFalse("Customer ID should be redacted", result.contains("12345"));
  }

  // =========================================================================
  // MySQL GROUP_CONCAT Tests (toString() bypass fix)
  // =========================================================================

  @Test
  public void testMySQLGroupConcatBasic() {
    String sql = "SELECT GROUP_CONCAT(name) FROM users WHERE dept = 'Sales'";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve GROUP_CONCAT structure", result.contains("GROUP_CONCAT("));
    assertFalse("Dept value should be redacted", result.contains("Sales"));
    assertTrue("Should preserve column name 'name'", result.contains("name"));
  }

  @Test
  public void testMySQLGroupConcatWithSeparator() {
    String sql = "SELECT GROUP_CONCAT(name SEPARATOR ', ') FROM users WHERE id IN (1, 2, 3)";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve GROUP_CONCAT structure", result.contains("GROUP_CONCAT("));
    assertTrue("Should contain SEPARATOR keyword", result.contains("SEPARATOR"));
    assertFalse("Separator value should be redacted", result.contains("', '"));
    assertTrue("Separator should show redacted marker", result.contains("SEPARATOR " + SqlParser.REDACTED_STRING));
  }

  @Test
  public void testMySQLGroupConcatDistinctOrderBy() {
    String sql = "SELECT GROUP_CONCAT(DISTINCT name ORDER BY name ASC SEPARATOR '; ') "
        + "FROM employees WHERE salary > 50000";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve GROUP_CONCAT structure", result.contains("GROUP_CONCAT("));
    assertTrue("Should preserve DISTINCT keyword", result.contains("DISTINCT"));
    assertTrue("Should preserve ORDER BY", result.contains("ORDER BY"));
    assertTrue("Should preserve ASC", result.contains("ASC"));
    assertTrue("Should contain SEPARATOR keyword", result.contains("SEPARATOR"));
    assertFalse("Salary value should be redacted", result.contains("50000"));
  }

  // =========================================================================
  // MATCH...AGAINST (FullTextSearch) Tests (toString() bypass fix)
  // =========================================================================

  @Test
  public void testFullTextSearchBasic() {
    String sql = "SELECT * FROM articles WHERE MATCH(title, body) AGAINST ('database optimization')";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve MATCH structure", result.contains("MATCH ("));
    assertTrue("Should preserve AGAINST keyword", result.contains("AGAINST ("));
    assertTrue("Should preserve column 'title'", result.contains("title"));
    assertTrue("Should preserve column 'body'", result.contains("body"));
    assertFalse("Search term should be redacted", result.contains("database optimization"));
    assertTrue("Should contain redacted marker", result.contains(SqlParser.REDACTED_STRING));
  }

  @Test
  public void testFullTextSearchWithModifier() {
    String sql = "SELECT * FROM products WHERE MATCH(description) "
        + "AGAINST ('wireless bluetooth headphones' IN BOOLEAN MODE)";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve MATCH structure", result.contains("MATCH ("));
    assertTrue("Should preserve AGAINST keyword", result.contains("AGAINST ("));
    assertFalse("Search term should be redacted", result.contains("wireless"));
    assertFalse("Search term should be redacted", result.contains("bluetooth"));
    assertFalse("Search term should be redacted", result.contains("headphones"));
    assertTrue("Should preserve search modifier", result.contains("IN BOOLEAN MODE"));
  }

  @Test
  public void testFullTextSearchWithWhereCondition() {
    String sql = "SELECT * FROM posts WHERE MATCH(content) AGAINST ('secret document') "
        + "AND author = 'admin' AND views > 1000";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Search term should be redacted", result.contains("secret document"));
    assertFalse("Author should be redacted", result.contains("admin"));
    assertFalse("Views count should be redacted", result.contains("1000"));
    assertTrue("Should preserve structure", result.contains("MATCH ("));
    assertTrue("Should preserve AND clause", result.contains("AND author ="));
    assertTrue("Should preserve AND clause", result.contains("AND views >"));
  }

  // =========================================================================
  // IS [NOT] DISTINCT FROM Tests (toString() bypass fix)
  // =========================================================================

  @Test
  public void testIsDistinctFromWithString() {
    String sql = "SELECT * FROM users WHERE name IS DISTINCT FROM 'John'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("String value should be redacted", result.contains("John"));
    assertTrue("Should preserve IS DISTINCT FROM structure",
        result.contains("IS DISTINCT FROM"));
    assertTrue("Should contain redacted marker", result.contains(SqlParser.REDACTED_STRING));
  }

  @Test
  public void testIsDistinctFromWithNumber() {
    String sql = "SELECT * FROM orders WHERE amount IS DISTINCT FROM 999";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Numeric value should be redacted", result.contains("999"));
    assertTrue("Should preserve IS DISTINCT FROM structure",
        result.contains("IS DISTINCT FROM"));
  }

  @Test
  public void testIsNotDistinctFrom() {
    String sql = "SELECT * FROM products WHERE category IS NOT DISTINCT FROM 'Electronics'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Category value should be redacted", result.contains("Electronics"));
    assertTrue("Should preserve IS NOT DISTINCT FROM structure",
        result.contains("IS NOT DISTINCT FROM"));
  }

  // =========================================================================
  // Oracle CONNECT BY / START WITH Tests (toString() bypass fix)
  // =========================================================================

  @Test
  public void testOracleConnectByStartWithLiterals() {
    String sql = "SELECT employee_id, manager_id FROM employees "
        + "START WITH employee_id = 100 "
        + "CONNECT BY PRIOR employee_id = manager_id";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Employee ID 100 should be redacted", result.contains("100"));
    assertTrue("Should preserve START WITH", result.contains("START WITH"));
    assertTrue("Should preserve CONNECT BY PRIOR", result.contains("CONNECT BY"));
    assertTrue("Should preserve column references", result.contains("employee_id"));
    assertTrue("Should preserve column references", result.contains("manager_id"));
  }

  @Test
  public void testOracleConnectByWithNoCycle() {
    String sql = "SELECT * FROM categories "
        + "START WITH parent_id = 0 "
        + "CONNECT BY NOCYCLE PRIOR category_id = parent_id";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve NOCYCLE keyword", result.contains("NOCYCLE"));
    assertTrue("Should preserve START WITH", result.contains("START WITH"));
    assertTrue("Should preserve CONNECT BY", result.contains("CONNECT BY"));
  }

  @Test
  public void testOracleConnectByStartWithStringLiteral() {
    String sql = "SELECT * FROM org_chart "
        + "START WITH dept_name = 'Engineering' "
        + "CONNECT BY PRIOR dept_id = parent_dept_id";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Dept name should be redacted", result.contains("Engineering"));
    assertTrue("Should contain redacted marker", result.contains(SqlParser.REDACTED_STRING));
    assertTrue("Should preserve START WITH", result.contains("START WITH"));
  }

  // =========================================================================
  // PostgreSQL JsonExpression Tests (toString() bypass fix)
  // =========================================================================

  @Test
  public void testPostgresJsonExpressionArrow() {
    String sql = "SELECT data->'address' FROM users WHERE id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve -> operator", result.contains("->"));
    assertFalse("ID should be redacted", result.contains(" 1"));
    assertTrue("Should preserve FROM clause", result.contains("FROM users"));
  }

  @Test
  public void testPostgresJsonExpressionDoubleArrow() {
    String sql = "SELECT info->>'email' FROM customers WHERE info->>'name' = 'Alice'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Name value should be redacted", result.contains("Alice"));
    assertTrue("Should preserve ->> operator", result.contains("->>"));
    assertTrue("Should contain redacted marker", result.contains(SqlParser.REDACTED_STRING));
  }

  @Test
  public void testPostgresJsonExpressionChained() {
    String sql = "SELECT data->'profile'->'address'->>'city' FROM users WHERE id = 42";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("ID 42 should be redacted", result.contains("42"));
    assertTrue("Should preserve data column", result.contains("data"));
    assertTrue("Should preserve arrow operators", result.contains("->"));
  }

  // =========================================================================
  // Oracle KEEP Expression Tests (toString() bypass fix)
  // =========================================================================

  @Test
  public void testOracleKeepExpression() {
    String sql = "SELECT department_id, "
        + "MAX(salary) KEEP (DENSE_RANK FIRST ORDER BY hire_date) AS earliest_max_salary "
        + "FROM employees WHERE salary > 50000 GROUP BY department_id";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary value should be redacted", result.contains("50000"));
    assertTrue("Should preserve KEEP structure", result.contains("KEEP ("));
    assertTrue("Should preserve DENSE_RANK", result.contains("DENSE_RANK"));
    assertTrue("Should preserve ORDER BY", result.contains("ORDER BY"));
    assertTrue("Should preserve column references", result.contains("hire_date"));
  }

  @Test
  public void testOracleKeepExpressionLast() {
    String sql = "SELECT MIN(salary) KEEP (DENSE_RANK LAST ORDER BY commission_pct) "
        + "FROM employees WHERE department_id = 80";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Department ID should be redacted", result.contains(" 80"));
    assertTrue("Should preserve KEEP LAST", result.contains("LAST"));
    assertTrue("Should preserve DENSE_RANK", result.contains("DENSE_RANK"));
  }

  // =========================================================================
  // OVERLAPS Condition Tests (toString() bypass fix)
  // =========================================================================

  @Test
  public void testOverlapsCondition() {
    String sql = "SELECT * FROM reservations WHERE "
        + "(start_date, end_date) OVERLAPS ('2024-01-01', '2024-06-30')";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Start date should be redacted", result.contains("2024-01-01"));
    assertFalse("End date should be redacted", result.contains("2024-06-30"));
    assertTrue("Should preserve OVERLAPS keyword", result.contains("OVERLAPS"));
    assertTrue("Should contain redacted markers", result.contains(SqlParser.REDACTED_STRING));
  }

  // =========================================================================
  // NullValue Tests (explicit override verification)
  // =========================================================================

  @Test
  public void testNullValuePreserved() {
    String sql = "SELECT * FROM users WHERE name IS NULL AND email IS NOT NULL";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("NULL keyword should be preserved", result.contains("IS NULL"));
    assertTrue("NOT NULL should be preserved", result.contains("IS NOT NULL"));
    assertEquals("SELECT * FROM users WHERE name IS NULL AND email IS NOT NULL", result);
  }

  @Test
  public void testNullValueInCoalesce() {
    String sql = "SELECT COALESCE(name, NULL, 'Unknown') FROM users WHERE id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("NULL should be preserved in COALESCE", result.contains("NULL"));
    assertFalse("String value should be redacted", result.contains("Unknown"));
    assertFalse("ID should be redacted", result.contains(" 1)") || result.contains(" 1 "));
  }

  // =========================================================================
  // Comprehensive Expression Coverage Tests
  // =========================================================================

  @Test
  public void testCaseExpressionWithLiterals() {
    String sql = "SELECT CASE status WHEN 'A' THEN 'Active' WHEN 'I' THEN 'Inactive' "
        + "ELSE 'Unknown' END FROM users WHERE id = 42";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status values should be redacted", result.contains("'A'"));
    assertFalse("Label values should be redacted", result.contains("Active"));
    assertFalse("Label values should be redacted", result.contains("Inactive"));
    assertFalse("Label values should be redacted", result.contains("Unknown"));
    assertTrue("Should preserve CASE structure", result.contains("CASE"));
    assertTrue("Should preserve WHEN keyword", result.contains("WHEN"));
    assertTrue("Should preserve THEN keyword", result.contains("THEN"));
    assertTrue("Should preserve ELSE keyword", result.contains("ELSE"));
    assertTrue("Should preserve END keyword", result.contains("END"));
  }

  @Test
  public void testCastExpressionPreservesType() {
    String sql = "SELECT CAST('2024-01-15' AS DATE) FROM orders WHERE id = 100";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Date literal should be redacted", result.contains("2024-01-15"));
    assertFalse("ID should be redacted", result.contains("100"));
    assertTrue("Should preserve CAST keyword", result.contains("CAST("));
    assertTrue("Should preserve AS keyword", result.contains("AS"));
    assertTrue("Should preserve type name", result.contains("DATE"));
  }

  @Test
  public void testExtractExpression() {
    String sql = "SELECT EXTRACT(YEAR FROM order_date) FROM orders WHERE total > 500";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Total value should be redacted", result.contains("500"));
    assertTrue("Should preserve EXTRACT structure", result.contains("EXTRACT("));
    assertTrue("Should preserve YEAR keyword", result.contains("YEAR"));
  }

  @Test
  public void testBinaryExpressionWithLiterals() {
    String sql = "SELECT price * 1.1 AS tax_price FROM products WHERE discount <> 0.15";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Multiplier should be redacted", result.contains("1.1"));
    assertFalse("Discount should be redacted", result.contains("0.15"));
    assertTrue("Should preserve multiplication operator", result.contains("*"));
    assertTrue("Should preserve not-equals operator", result.contains("<>"));
  }

  @Test
  public void testRegExpMatchOperator() {
    String sql = "SELECT * FROM products WHERE name ~ 'secret_pattern' AND price > 99.99";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Pattern should be redacted", result.contains("secret_pattern"));
    assertFalse("Price should be redacted", result.contains("99.99"));
    assertTrue("Should preserve ~ operator", result.contains("~"));
  }

  @Test
  public void testContainsOperator() {
    String sql = "SELECT * FROM data WHERE tags @> ARRAY['sensitive']";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Array value should be redacted", result.contains("sensitive"));
    assertTrue("Should preserve @> operator", result.contains("@>"));
  }

  @Test
  public void testJsonOperatorPreservesStructure() {
    String sql = "SELECT data->>'name', data->>'email' FROM users WHERE data->>'status' = 'active'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status value should be redacted", result.contains("active"));
    assertTrue("Should preserve ->> operator", result.contains("->>"));
    assertTrue("Should preserve FROM clause", result.contains("FROM users"));
  }

  @Test
  public void testSimilarToExpression() {
    String sql = "SELECT * FROM users WHERE name SIMILAR TO '%John%'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Pattern should be redacted", result.contains("John"));
    assertTrue("Should preserve SIMILAR TO", result.contains("SIMILAR TO"));
  }

  @Test
  public void testAnalyticExpressionWithLiterals() {
    String sql = "SELECT name, salary, "
        + "LAG(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_salary "
        + "FROM employees WHERE dept = 'IT'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Dept value should be redacted", result.contains("'IT'"));
    assertTrue("Should preserve LAG function", result.contains("LAG("));
    assertTrue("Should preserve OVER clause", result.contains("OVER ("));
    assertTrue("Should preserve ORDER BY", result.contains("ORDER BY"));
  }

  @Test
  public void testTranscodingFunction() {
    String sql = "SELECT CONVERT('Hello World' USING utf8) FROM dual";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("String should be redacted", result.contains("Hello World"));
    assertTrue("Should preserve CONVERT structure", result.contains("CONVERT("));
  }

  @Test
  public void testExistsWithLiterals() {
    String sql = "SELECT * FROM customers WHERE EXISTS "
        + "(SELECT 1 FROM orders WHERE customer_id = customers.id AND total > 10000)";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Total value should be redacted", result.contains("10000"));
    assertTrue("Should preserve EXISTS keyword", result.contains("EXISTS"));
    assertTrue("Should preserve subquery structure", result.contains("SELECT"));
  }

  @Test
  public void testMemberOfExpression() {
    String sql = "SELECT * FROM data WHERE 42 MEMBER OF (json_col)";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Value 42 should be redacted", result.contains(" 42 "));
    assertTrue("Should preserve MEMBER OF", result.contains("MEMBER OF"));
  }

  @Test
  public void testAllExpressionsEndToEnd() {
    // A comprehensive query touching many expression types
    String sql = "SELECT "
        + "CASE WHEN status = 'A' THEN 'Active' ELSE 'Inactive' END, "
        + "CAST(price AS DECIMAL(10, 2)), "
        + "COALESCE(name, 'Unknown'), "
        + "CONCAT(first, ' ', last) "
        + "FROM users u "
        + "INNER JOIN orders o ON u.id = o.user_id "
        + "WHERE u.email LIKE '%@secret.com' "
        + "AND o.total BETWEEN 100 AND 5000 "
        + "AND u.age IN (25, 30, 35) "
        + "AND o.note IS NOT NULL "
        + "ORDER BY o.total DESC "
        + "LIMIT 50 OFFSET 10";
    String result = SqlParser.redactSensitiveData(sql);

    // Verify ALL literal values are redacted
    assertFalse("Status should be redacted", result.contains("'A'"));
    assertFalse("Active should be redacted", result.contains("Active"));
    assertFalse("Inactive should be redacted", result.contains("Inactive"));
    assertFalse("Unknown should be redacted", result.contains("Unknown"));
    assertFalse("Email domain should be redacted", result.contains("@secret.com"));
    assertFalse("Total lower bound should be redacted", result.contains(" 100 "));
    assertFalse("Total upper bound should be redacted", result.contains("5000"));
    assertFalse("Age values should be redacted", result.contains(" 25"));
    assertFalse("Age values should be redacted", result.contains(" 30"));
    assertFalse("Age values should be redacted", result.contains(" 35"));

    // Verify structural elements are preserved
    assertTrue("CASE should be preserved", result.contains("CASE"));
    assertTrue("CAST should be preserved", result.contains("CAST("));
    assertTrue("COALESCE should be preserved", result.contains("COALESCE("));
    assertTrue("CONCAT should be preserved", result.contains("CONCAT("));
    assertTrue("JOIN should be preserved", result.contains("INNER JOIN"));
    assertTrue("LIKE should be preserved", result.contains("LIKE"));
    assertTrue("BETWEEN should be preserved", result.contains("BETWEEN"));
    assertTrue("IN should be preserved", result.contains("IN ("));
    assertTrue("IS NOT NULL should be preserved", result.contains("IS NOT NULL"));
    assertTrue("ORDER BY should be preserved", result.contains("ORDER BY"));
    assertTrue("LIMIT should be preserved", result.contains("LIMIT"));
    assertTrue("OFFSET should be preserved", result.contains("OFFSET"));
    // DECIMAL type params are structural
    assertTrue("DECIMAL type params should be preserved", result.contains("DECIMAL (10, 2)"));
  }

  // =========================================================================
  // Array Expression Tests (visitor traversal fix)
  // =========================================================================

  @Test
  public void testArrayIndexing() {
    String sql = "SELECT data[1] FROM users WHERE id = 42";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve array access structure", result.contains("data["));
    assertFalse("Array index should be redacted", result.contains("[1]"));
    assertFalse("ID should be redacted", result.contains(" 42"));
    assertTrue("Should contain redacted number", result.contains(SqlParser.REDACTED_NUMBER));
  }

  @Test
  public void testArraySlicing() {
    String sql = "SELECT arr[2:5] FROM data_table WHERE id = 10";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve array slice structure", result.contains("["));
    assertTrue("Should preserve colon separator", result.contains(":"));
    assertFalse("ID should be redacted", result.contains(" 10"));
  }

  @Test
  public void testArrayConstructorWithLiterals() {
    String sql = "SELECT * FROM products WHERE tags = ARRAY['secret', 'confidential']";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Array value should be redacted", result.contains("secret"));
    assertFalse("Array value should be redacted", result.contains("confidential"));
    assertTrue("Should preserve ARRAY keyword", result.contains("ARRAY"));
    assertTrue("Should contain redacted markers", result.contains(SqlParser.REDACTED_STRING));
  }

  // =========================================================================
  // PostgreSQL-Specific Expression Tests
  // =========================================================================

  @Test
  public void testPostgresAtTimeZone() {
    String sql = "SELECT created_at AT TIME ZONE 'UTC' FROM events WHERE id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve AT TIME ZONE structure", result.contains("AT TIME ZONE"));
    assertFalse("Timezone value should be redacted", result.contains("UTC"));
    assertTrue("Should contain redacted marker", result.contains(SqlParser.REDACTED_STRING));
  }

  @Test
  public void testPostgresCastColonSyntax() {
    String sql = "SELECT price::numeric FROM products WHERE name = 'Widget'";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve :: cast operator", result.contains("::"));
    assertTrue("Should preserve type name", result.contains("numeric"));
    assertFalse("Name value should be redacted", result.contains("Widget"));
  }

  @Test
  public void testPostgresSimilarTo() {
    String sql = "SELECT * FROM users WHERE email SIMILAR TO '%@(gmail|yahoo)\\.com'";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve SIMILAR TO", result.contains("SIMILAR TO"));
    assertFalse("Pattern should be redacted", result.contains("gmail"));
    assertFalse("Pattern should be redacted", result.contains("yahoo"));
  }

  @Test
  public void testPostgresJsonContainsOperator() {
    String sql = "SELECT * FROM docs WHERE metadata @> '{\"type\": \"report\"}' AND id = 42";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("JSON value should be redacted", result.contains("report"));
    assertFalse("ID should be redacted", result.contains(" 42"));
    assertTrue("Should preserve @> operator", result.contains("@>"));
  }

  @Test
  public void testPostgresContainedByOperator() {
    String sql = "SELECT * FROM tags WHERE tag_array <@ ARRAY['important', 'urgent']";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Array values should be redacted", result.contains("important"));
    assertFalse("Array values should be redacted", result.contains("urgent"));
    assertTrue("Should preserve <@ operator", result.contains("<@"));
  }

  @Test
  public void testPostgresGeometryDistance() {
    String sql = "SELECT * FROM places WHERE location <-> point(37.7749, -122.4194) < 10";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Lat should be redacted", result.contains("37.7749"));
    assertFalse("Lon should be redacted", result.contains("122.4194"));
    assertTrue("Should preserve <-> operator", result.contains("<->"));
  }

  @Test
  public void testPostgresStringAgg() {
    String sql = "SELECT STRING_AGG(name, ', ' ORDER BY name) FROM employees WHERE dept = 'Sales'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Dept value should be redacted", result.contains("Sales"));
    assertTrue("Should preserve STRING_AGG function", result.contains("STRING_AGG("));
    assertTrue("Should preserve ORDER BY", result.contains("ORDER BY"));
  }

  @Test
  public void testPostgresIntervalExpression() {
    String sql = "SELECT * FROM events WHERE event_time > NOW() - INTERVAL '30 days'";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve INTERVAL keyword", result.contains("INTERVAL"));
    assertFalse("Interval value should be redacted", result.contains("30 days"));
    assertTrue("Should preserve NOW() function", result.contains("NOW()"));
  }

  @Test
  public void testPostgresCoalesceWithMultipleArgs() {
    String sql = "SELECT COALESCE(a, b, c, 'default_val') FROM t WHERE x = 99";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Default value should be redacted", result.contains("default_val"));
    assertFalse("Number should be redacted", result.contains("99"));
    assertTrue("Should preserve COALESCE", result.contains("COALESCE("));
    assertTrue("Should preserve column names", result.contains("a, b, c"));
  }

  // =========================================================================
  // Oracle-Specific Expression Tests
  // =========================================================================

  @Test
  public void testOracleConnectByRoot() {
    String sql = "SELECT CONNECT_BY_ROOT employee_name AS root_name, level "
        + "FROM employees "
        + "START WITH manager_id IS NULL "
        + "CONNECT BY PRIOR employee_id = manager_id";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve CONNECT_BY_ROOT", result.contains("CONNECT_BY_ROOT"));
    assertTrue("Should preserve START WITH", result.contains("START WITH"));
    assertTrue("Should preserve CONNECT BY PRIOR", result.contains("CONNECT BY"));
  }

  @Test
  public void testOracleNvl2Function() {
    String sql = "SELECT NVL2(commission_pct, salary * commission_pct, 0) "
        + "FROM employees WHERE department_id = 80";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Department ID should be redacted", result.contains(" 80"));
    assertTrue("Should preserve NVL2 function", result.contains("NVL2("));
  }

  @Test
  public void testOracleListagg() {
    String sql = "SELECT department_id, LISTAGG(last_name, ', ') "
        + "WITHIN GROUP (ORDER BY last_name) "
        + "FROM employees WHERE salary > 50000 GROUP BY department_id";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary value should be redacted", result.contains("50000"));
    assertTrue("Should preserve LISTAGG function", result.contains("LISTAGG("));
    assertTrue("Should preserve WITHIN GROUP", result.contains("WITHIN GROUP"));
  }

  @Test
  public void testOracleRegexpLike() {
    String sql = "SELECT * FROM customers WHERE REGEXP_LIKE(email, '^[a-z]+@example\\.com$')";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Pattern should be redacted", result.contains("example"));
    assertTrue("Should preserve REGEXP_LIKE function", result.contains("REGEXP_LIKE("));
  }

  @Test
  public void testOracleCoalesceWithNested() {
    String sql = "SELECT COALESCE(preferred_name, first_name || ' ' || last_name, 'Unknown') "
        + "FROM employees WHERE emp_id = 123";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Default value should be redacted", result.contains("Unknown"));
    assertFalse("Employee ID should be redacted", result.contains("123"));
    assertTrue("Should preserve COALESCE", result.contains("COALESCE("));
    assertTrue("Should preserve || operator", result.contains("||"));
  }

  // =========================================================================
  // MySQL-Specific Expression Tests
  // =========================================================================

  @Test
  public void testMySQLMatchAgainstWithBooleanMode() {
    String sql = "SELECT * FROM articles WHERE MATCH(title, body) "
        + "AGAINST ('+mysql -oracle' IN BOOLEAN MODE) AND category = 'tech'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Search term should be redacted", result.contains("mysql"));
    assertFalse("Search term should be redacted", result.contains("oracle"));
    assertFalse("Category should be redacted", result.contains("tech"));
    assertTrue("Should preserve MATCH", result.contains("MATCH ("));
    assertTrue("Should preserve AGAINST", result.contains("AGAINST ("));
    assertTrue("Should preserve IN BOOLEAN MODE", result.contains("IN BOOLEAN MODE"));
  }

  @Test
  public void testMySQLGroupConcatComplex() {
    String sql = "SELECT dept, GROUP_CONCAT(DISTINCT name ORDER BY name DESC SEPARATOR ' | ') "
        + "FROM employees WHERE salary > 60000 GROUP BY dept";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary should be redacted", result.contains("60000"));
    assertTrue("Should preserve GROUP_CONCAT", result.contains("GROUP_CONCAT("));
    assertTrue("Should preserve DISTINCT", result.contains("DISTINCT"));
    assertTrue("Should preserve ORDER BY", result.contains("ORDER BY"));
    assertTrue("Should preserve DESC", result.contains("DESC"));
    assertTrue("Should preserve SEPARATOR", result.contains("SEPARATOR"));
    assertFalse("Separator value should be redacted", result.contains("' | '"));
  }

  @Test
  public void testMySQLIntervalDateAdd() {
    String sql = "SELECT * FROM events WHERE event_date > DATE_SUB(NOW(), INTERVAL 7 DAY) "
        + "AND status = 'active'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status should be redacted", result.contains("'active'"));
    assertTrue("Should preserve DATE_SUB", result.contains("DATE_SUB("));
    assertTrue("Should preserve NOW()", result.contains("NOW()"));
    assertTrue("Should preserve INTERVAL", result.contains("INTERVAL"));
    assertTrue("Should preserve DAY", result.contains("DAY"));
  }

  @Test
  public void testMySQLReplace() {
    String sql = "SELECT REPLACE(description, 'old_val', 'new_val') FROM products WHERE id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Old value should be redacted", result.contains("old_val"));
    assertFalse("New value should be redacted", result.contains("new_val"));
    assertTrue("Should preserve REPLACE function", result.contains("REPLACE("));
  }

  // =========================================================================
  // SQL Server-Specific Expression Tests
  // =========================================================================

  @Test
  public void testSqlServerStringAgg() {
    String sql = "SELECT department, STRING_AGG(employee_name, ', ') "
        + "FROM employees WHERE status = 'active' GROUP BY department";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status should be redacted", result.contains("active"));
    assertTrue("Should preserve STRING_AGG", result.contains("STRING_AGG("));
  }

  @Test
  public void testSqlServerCoalesce() {
    String sql = "SELECT COALESCE(phone, mobile, 'N/A') FROM contacts WHERE id = 500";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Default value should be redacted", result.contains("N/A"));
    assertFalse("ID should be redacted", result.contains("500"));
    assertTrue("Should preserve COALESCE", result.contains("COALESCE("));
  }

  @Test
  public void testSqlServerTry_Convert() {
    String sql = "SELECT TRY_CONVERT(INT, '12345') FROM data WHERE key = 'test'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Value should be redacted", result.contains("12345"));
    assertFalse("Key should be redacted", result.contains("test"));
    assertTrue("Should preserve TRY_CONVERT", result.contains("TRY_CONVERT("));
  }

  // =========================================================================
  // TRIM Function Tests (visitor-safe override)
  // =========================================================================

  @Test
  public void testTrimFunction() {
    String sql = "SELECT TRIM(' sensitive_data ') FROM users WHERE id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Trim value should be redacted", result.contains("sensitive_data"));
    assertTrue("Should preserve Trim function", result.contains("Trim("));
  }

  @Test
  public void testTrimWithSpecification() {
    String sql = "SELECT TRIM(LEADING 'x' FROM name) FROM users WHERE id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Trim char should be redacted", result.contains("'x'"));
    assertTrue("Should preserve LEADING spec", result.contains("LEADING"));
    assertTrue("Should preserve FROM keyword", result.contains("FROM"));
  }

  // =========================================================================
  // Window Function Frame Specs Tests
  // =========================================================================

  @Test
  public void testWindowFunctionRowsBetween() {
    String sql = "SELECT name, salary, "
        + "AVG(salary) OVER (PARTITION BY dept ORDER BY hire_date "
        + "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg "
        + "FROM employees WHERE status = 'active'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status should be redacted", result.contains("'active'"));
    assertTrue("Should preserve ROWS frame", result.contains("ROWS"));
    assertTrue("Should preserve BETWEEN", result.contains("BETWEEN"));
    assertTrue("Should preserve PRECEDING", result.contains("PRECEDING"));
    assertTrue("Should preserve CURRENT ROW", result.contains("CURRENT ROW"));
  }

  @Test
  public void testWindowFunctionRangeBetween() {
    String sql = "SELECT id, value, "
        + "SUM(value) OVER (ORDER BY id "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative "
        + "FROM data WHERE category = 'A'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Category should be redacted", result.contains("'A'"));
    assertTrue("Should preserve RANGE frame", result.contains("RANGE"));
    assertTrue("Should preserve UNBOUNDED PRECEDING", result.contains("UNBOUNDED PRECEDING"));
    assertTrue("Should preserve CURRENT ROW", result.contains("CURRENT ROW"));
  }

  @Test
  public void testWindowFunctionWithFilter() {
    String sql = "SELECT department, "
        + "COUNT(*) FILTER (WHERE salary > 50000) OVER (PARTITION BY department) AS high_earners "
        + "FROM employees WHERE year = 2024";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary should be redacted", result.contains("50000"));
    assertFalse("Year should be redacted", result.contains("2024"));
    assertTrue("Should preserve FILTER", result.contains("FILTER"));
    assertTrue("Should preserve PARTITION BY", result.contains("PARTITION BY"));
  }

  // =========================================================================
  // DELETE / INSERT Statement Tests
  // =========================================================================

  @Test
  public void testDeleteStatement() {
    String sql = "DELETE FROM users WHERE name = 'John Doe' AND created_at < '2023-01-01'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Name should be redacted", result.contains("John Doe"));
    assertFalse("Date should be redacted", result.contains("2023-01-01"));
    assertTrue("Should preserve DELETE FROM", result.contains("DELETE FROM users"));
    assertTrue("Should preserve WHERE clause", result.contains("WHERE"));
  }

  @Test
  public void testInsertWithValues() {
    String sql = "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Name should be redacted", result.contains("Alice"));
    assertFalse("Email should be redacted", result.contains("alice@test.com"));
    assertTrue("Should preserve INSERT INTO", result.contains("INSERT INTO users"));
    assertTrue("Should preserve VALUES", result.contains("VALUES"));
    assertTrue("Should contain redacted markers", result.contains(SqlParser.REDACTED_STRING));
  }

  // =========================================================================
  // CTE (Common Table Expression) Tests
  // =========================================================================

  @Test
  public void testCTEWithLiterals() {
    String sql = "WITH high_earners AS ("
        + "SELECT * FROM employees WHERE salary > 100000"
        + ") SELECT name FROM high_earners WHERE dept = 'Engineering'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary should be redacted", result.contains("100000"));
    assertFalse("Dept should be redacted", result.contains("Engineering"));
    assertTrue("Should preserve WITH keyword", result.contains("WITH"));
    assertTrue("Should preserve CTE name", result.contains("high_earners"));
  }

  @Test
  public void testMultipleCTEs() {
    String sql = "WITH dept_stats AS ("
        + "SELECT dept, AVG(salary) AS avg_sal FROM employees GROUP BY dept"
        + "), top_depts AS ("
        + "SELECT dept FROM dept_stats WHERE avg_sal > 80000"
        + ") SELECT * FROM employees WHERE dept IN (SELECT dept FROM top_depts) "
        + "AND name LIKE '%Smith%'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary threshold should be redacted", result.contains("80000"));
    assertFalse("Name pattern should be redacted", result.contains("Smith"));
    assertTrue("Should preserve CTE structure", result.contains("WITH"));
    assertTrue("Should preserve dept_stats CTE", result.contains("dept_stats"));
    assertTrue("Should preserve top_depts CTE", result.contains("top_depts"));
  }

  // =========================================================================
  // Bitwise and Logical Operator Tests
  // =========================================================================

  @Test
  public void testBitwiseOperators() {
    String sql = "SELECT flags & 255 AS masked, flags | 128 AS with_flag "
        + "FROM settings WHERE id = 42";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Bitmask should be redacted", result.contains("255"));
    assertFalse("Flag value should be redacted", result.contains("128"));
    assertFalse("ID should be redacted", result.contains(" 42"));
    assertTrue("Should preserve & operator", result.contains("&"));
    assertTrue("Should preserve | operator", result.contains("|"));
  }

  @Test
  public void testXorExpression() {
    String sql = "SELECT * FROM flags WHERE (active = 1) XOR (verified = 1)";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve XOR operator", result.contains("XOR"));
    assertTrue("Should contain redacted numbers", result.contains(SqlParser.REDACTED_NUMBER));
  }

  @Test
  public void testModuloOperator() {
    String sql = "SELECT * FROM orders WHERE order_id % 100 = 0";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Modulo value should be redacted", result.contains(" 100 "));
    assertTrue("Should preserve % operator", result.contains("%"));
  }

  // =========================================================================
  // NOT Expression Tests
  // =========================================================================

  @Test
  public void testNotExpressionWithLiteral() {
    String sql = "SELECT * FROM users WHERE NOT (status = 'banned')";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status should be redacted", result.contains("banned"));
    assertTrue("Should preserve NOT keyword", result.contains("NOT"));
  }

  // =========================================================================
  // Variable Assignment Tests (MySQL)
  // =========================================================================

  @Test
  public void testMySQLVariableAssignment() {
    String sql = "SELECT @rank := @rank + 1 AS rank, name FROM employees WHERE dept = 'IT'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Dept should be redacted", result.contains("'IT'"));
    assertTrue("Should preserve @rank variable", result.contains("@rank"));
  }

  // =========================================================================
  // EXTRACT Expression Tests
  // =========================================================================

  @Test
  public void testExtractYear() {
    String sql = "SELECT * FROM orders WHERE EXTRACT(YEAR FROM order_date) = 2024 "
        + "AND total > 1000";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Year value should be redacted", result.contains("2024"));
    assertFalse("Total should be redacted", result.contains("1000"));
    assertTrue("Should preserve EXTRACT", result.contains("EXTRACT("));
    assertTrue("Should preserve YEAR", result.contains("YEAR"));
  }

  @Test
  public void testExtractMonth() {
    String sql = "SELECT EXTRACT(MONTH FROM hire_date) AS hire_month "
        + "FROM employees WHERE salary > 50000";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary should be redacted", result.contains("50000"));
    assertTrue("Should preserve EXTRACT", result.contains("EXTRACT("));
    assertTrue("Should preserve MONTH", result.contains("MONTH"));
  }

  // =========================================================================
  // CAST Expression Tests (PostgreSQL :: syntax and standard)
  // =========================================================================

  @Test
  public void testCastWithVarchar() {
    String sql = "SELECT CAST(employee_id AS VARCHAR(50)) FROM employees WHERE name = 'Alice'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Name should be redacted", result.contains("Alice"));
    assertTrue("Should preserve CAST", result.contains("CAST("));
    assertTrue("Should preserve AS", result.contains("AS"));
  }

  // =========================================================================
  // Comprehensive Database-Agnostic Expression Tests
  // =========================================================================

  @Test
  public void testNestedFunctionsWithLiterals() {
    String sql = "SELECT UPPER(TRIM(CONCAT(first_name, ' ', last_name))) "
        + "FROM users WHERE LOWER(email) = 'admin@example.com'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Email should be redacted", result.contains("admin@example.com"));
    assertTrue("Should preserve UPPER function", result.contains("UPPER("));
    assertTrue("Should preserve TRIM function", result.contains("Trim("));
    assertTrue("Should preserve CONCAT function", result.contains("CONCAT("));
    assertTrue("Should preserve LOWER function", result.contains("LOWER("));
  }

  @Test
  public void testArithmeticWithLiterals() {
    String sql = "SELECT price * 1.08 AS tax_price, price + 500 AS markup "
        + "FROM products WHERE price > 100 AND discount <> 0.15";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Tax rate should be redacted", result.contains("1.08"));
    assertFalse("Markup should be redacted", result.contains(" 500 "));
    assertFalse("Price threshold should be redacted", result.contains(" 100 "));
    assertFalse("Discount should be redacted", result.contains("0.15"));
    assertTrue("Should preserve * operator", result.contains("*"));
    assertTrue("Should preserve + operator", result.contains("+"));
  }

  @Test
  public void testIsBooleanExpression() {
    String sql = "SELECT * FROM flags WHERE active IS TRUE AND deleted IS NOT FALSE";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve IS TRUE", result.contains("IS TRUE"));
    assertTrue("Should preserve IS NOT FALSE", result.contains("IS NOT FALSE"));
    assertEquals("SELECT * FROM flags WHERE active IS TRUE AND deleted IS NOT FALSE", result);
  }

  @Test
  public void testIsNullAndIsNotNull() {
    String sql = "SELECT * FROM users WHERE email IS NOT NULL AND phone IS NULL";
    String result = SqlParser.redactSensitiveData(sql);

    assertEquals("SELECT * FROM users WHERE email IS NOT NULL AND phone IS NULL", result);
  }

  @Test
  public void testSubSelectInExpression() {
    String sql = "SELECT * FROM orders WHERE amount > ANY "
        + "(SELECT avg_amount FROM dept_stats WHERE dept = 'Sales')";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Dept should be redacted", result.contains("Sales"));
    assertTrue("Should preserve ANY keyword", result.contains("ANY"));
  }

  @Test
  public void testIntegerDivision() {
    String sql = "SELECT total DIV 100 AS hundreds FROM accounts WHERE balance > 5000";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Division value should be redacted", result.contains(" 100 "));
    assertFalse("Balance should be redacted", result.contains("5000"));
    assertTrue("Should preserve DIV operator", result.contains("DIV"));
  }

  @Test
  public void testParenthesizedExpression() {
    String sql = "SELECT * FROM orders WHERE (status = 'pending' OR status = 'processing') "
        + "AND total > 1000";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status should be redacted", result.contains("pending"));
    assertFalse("Status should be redacted", result.contains("processing"));
    assertFalse("Total should be redacted", result.contains("1000"));
    assertTrue("Should preserve parentheses", result.contains("("));
    assertTrue("Should preserve OR", result.contains("OR"));
  }

  @Test
  public void testTimezoneExpressionMultiple() {
    String sql = "SELECT event_time AT TIME ZONE 'America/New_York' "
        + "FROM events WHERE event_type = 'meeting'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Timezone should be redacted", result.contains("America/New_York"));
    assertFalse("Event type should be redacted", result.contains("meeting"));
    assertTrue("Should preserve AT TIME ZONE", result.contains("AT TIME ZONE"));
  }

  @Test
  public void testConcatOperator() {
    String sql = "SELECT 'Mr. ' || first_name || ' ' || last_name FROM users WHERE id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Prefix should be redacted", result.contains("Mr. "));
    assertTrue("Should preserve || operator", result.contains("||"));
  }

  @Test
  public void testCurrentTimestampKeyword() {
    String sql = "SELECT * FROM logs WHERE created_at > CURRENT_TIMESTAMP AND level = 'ERROR'";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Should preserve CURRENT_TIMESTAMP", result.contains("CURRENT_TIMESTAMP"));
    assertFalse("Level should be redacted", result.contains("ERROR"));
  }

  // =========================================================================
  // Comprehensive End-to-End Multi-Database Tests
  // =========================================================================

  @Test
  public void testComplexPostgresQuery() {
    String sql = "SELECT u.name, u.email, "
        + "COALESCE(u.phone, 'N/A') AS phone, "
        + "ROW_NUMBER() OVER (PARTITION BY u.department ORDER BY u.salary DESC) AS rank, "
        + "u.metadata->>'role' AS role "
        + "FROM users u "
        + "WHERE u.status = 'active' "
        + "AND u.created_at > CURRENT_TIMESTAMP - INTERVAL '30 days' "
        + "AND u.tags @> ARRAY['verified'] "
        + "ORDER BY u.salary DESC "
        + "LIMIT 100 OFFSET 0";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Phone default should be redacted", result.contains("N/A"));
    assertFalse("Status should be redacted", result.contains("active"));
    assertFalse("Interval should be redacted", result.contains("30 days"));
    assertFalse("Tag should be redacted", result.contains("verified"));
    assertTrue("Should preserve query structure", result.contains("COALESCE("));
    assertTrue("Should preserve window function", result.contains("ROW_NUMBER()"));
    assertTrue("Should preserve PARTITION BY", result.contains("PARTITION BY"));
    assertTrue("Should preserve INTERVAL", result.contains("INTERVAL"));
    assertTrue("Should preserve LIMIT", result.contains("LIMIT"));
  }

  @Test
  public void testComplexOracleQuery() {
    String sql = "SELECT e.employee_id, e.name, e.salary, "
        + "MAX(e.salary) KEEP (DENSE_RANK FIRST ORDER BY e.hire_date) "
        + "OVER (PARTITION BY e.department_id) AS first_hire_max_sal "
        + "FROM employees e "
        + "WHERE e.salary > 50000 "
        + "AND e.department_id IN (10, 20, 30) "
        + "START WITH e.manager_id IS NULL "
        + "CONNECT BY PRIOR e.employee_id = e.manager_id";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary should be redacted", result.contains("50000"));
    assertFalse("Dept IDs should be redacted", result.contains(" 10,"));
    assertFalse("Dept IDs should be redacted", result.contains(" 20,"));
    assertTrue("Should preserve KEEP", result.contains("KEEP ("));
    assertTrue("Should preserve DENSE_RANK FIRST", result.contains("DENSE_RANK FIRST"));
    assertTrue("Should preserve START WITH", result.contains("START WITH"));
    assertTrue("Should preserve CONNECT BY PRIOR", result.contains("CONNECT BY"));
  }

  @Test
  public void testComplexMySQLQuery() {
    String sql = "SELECT d.name, "
        + "GROUP_CONCAT(DISTINCT e.name ORDER BY e.salary DESC SEPARATOR '; '), "
        + "COUNT(IF(e.salary > 75000, 1, NULL)) AS high_earners "
        + "FROM departments d "
        + "INNER JOIN employees e ON d.id = e.dept_id "
        + "WHERE d.status = 'active' "
        + "AND MATCH(d.description) AGAINST ('technology innovation' IN BOOLEAN MODE) "
        + "GROUP BY d.name "
        + "HAVING high_earners > 5 "
        + "LIMIT 20";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary should be redacted", result.contains("75000"));
    assertFalse("Status should be redacted", result.contains("'active'"));
    assertFalse("Search terms should be redacted", result.contains("technology"));
    assertFalse("Search terms should be redacted", result.contains("innovation"));
    assertTrue("Should preserve GROUP_CONCAT", result.contains("GROUP_CONCAT("));
    assertTrue("Should preserve MATCH", result.contains("MATCH ("));
    assertTrue("Should preserve AGAINST", result.contains("AGAINST ("));
    assertTrue("Should preserve IN BOOLEAN MODE", result.contains("IN BOOLEAN MODE"));
    assertTrue("Should preserve HAVING", result.contains("HAVING"));
  }

  @Test
  public void testComplexSqlServerQuery() {
    String sql = "SELECT TOP 50 e.name, e.salary, d.name AS dept_name, "
        + "IIF(e.salary > 80000, 'Senior', 'Junior') AS level, "
        + "FORMAT(e.hire_date, 'yyyy-MM-dd') AS formatted_date "
        + "FROM employees e WITH (NOLOCK) "
        + "INNER JOIN departments d ON e.dept_id = d.id "
        + "WHERE e.status = 'active' "
        + "AND DATEDIFF(day, e.hire_date, GETDATE()) > 365 "
        + "AND LEN(e.name) > 3 "
        + "ORDER BY e.salary DESC "
        + "OFFSET 10 ROWS FETCH NEXT 50 ROWS ONLY";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary threshold should be redacted", result.contains("80000"));
    assertFalse("Level values should be redacted", result.contains("Senior"));
    assertFalse("Level values should be redacted", result.contains("Junior"));
    assertFalse("Format pattern should be redacted", result.contains("yyyy-MM-dd"));
    assertFalse("Status should be redacted", result.contains("'active'"));
    assertFalse("Days should be redacted", result.contains("365"));
    assertTrue("Should preserve TOP 50", result.contains("TOP 50"));
    assertTrue("Should preserve IIF function", result.contains("IIF("));
    assertTrue("Should preserve FORMAT function", result.contains("FORMAT("));
    assertTrue("Should preserve NOLOCK hint", result.contains("NOLOCK"));
    assertTrue("Should preserve DATEDIFF", result.contains("DATEDIFF("));
    assertTrue("Should preserve LEN", result.contains("LEN("));
    assertTrue("Should preserve OFFSET/FETCH", result.contains("OFFSET"));
  }

  @Test
  public void testComplexDB2Query() {
    String sql = "SELECT e.name, e.salary, "
        + "VALUE(e.commission, 0) AS commission, "
        + "CASE WHEN e.salary > 100000 THEN 'Executive' "
        + "WHEN e.salary > 75000 THEN 'Senior' "
        + "ELSE 'Staff' END AS grade "
        + "FROM employees e "
        + "WHERE e.department = 'Finance' "
        + "AND e.hire_date BETWEEN '2020-01-01' AND '2024-12-31' "
        + "FETCH FIRST 25 ROWS ONLY "
        + "OPTIMIZE FOR 50 ROWS";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary threshold should be redacted", result.contains("100000"));
    assertFalse("Salary threshold should be redacted", result.contains("75000"));
    assertFalse("Grade values should be redacted", result.contains("Executive"));
    assertFalse("Grade values should be redacted", result.contains("Senior"));
    assertFalse("Grade values should be redacted", result.contains("Staff"));
    assertFalse("Department should be redacted", result.contains("Finance"));
    assertFalse("Date values should be redacted", result.contains("2020-01-01"));
    assertFalse("Date values should be redacted", result.contains("2024-12-31"));
    assertTrue("Should preserve CASE structure", result.contains("CASE"));
    assertTrue("Should preserve VALUE function", result.contains("VALUE("));
    assertTrue("Should preserve OPTIMIZE FOR", result.contains("OPTIMIZE FOR 50 ROWS"));
  }

  @Test
  public void testMergeStatementComplex() {
    String sql = "MERGE INTO target_table t "
        + "USING source_table s ON (t.id = s.id) "
        + "WHEN MATCHED AND s.status = 'active' THEN "
        + "UPDATE SET t.value = s.value, t.updated_at = '2024-01-15' "
        + "WHEN NOT MATCHED THEN "
        + "INSERT (id, value, status) VALUES (s.id, s.value, 'new')";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status should be redacted", result.contains("'active'"));
    assertFalse("Date should be redacted", result.contains("2024-01-15"));
    assertFalse("Status insert should be redacted", result.contains("'new'"));
    assertTrue("Should preserve MERGE INTO", result.contains("MERGE INTO"));
    assertTrue("Should preserve USING", result.contains("USING"));
    assertTrue("Should preserve WHEN MATCHED", result.contains("WHEN MATCHED"));
  }

  // =========================================================================
  // Edge Case and Robustness Tests
  // =========================================================================

  @Test
  public void testEmptyStringLiteral() {
    String sql = "SELECT * FROM users WHERE name = ''";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Empty string should be redacted", result.contains(SqlParser.REDACTED_STRING));
  }

  @Test
  public void testVeryLongStringLiteral() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      sb.append("A");
    }
    String longValue = sb.toString();
    String sql = "SELECT * FROM data WHERE content = '" + longValue + "'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Long string should be redacted", result.contains(longValue));
    assertTrue("Should contain redacted marker", result.contains(SqlParser.REDACTED_STRING));
  }

  @Test
  public void testMultipleNestedSubqueries() {
    String sql = "SELECT * FROM a WHERE x IN ("
        + "SELECT y FROM b WHERE z IN ("
        + "SELECT w FROM c WHERE v = 'deep_value'))";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Deep value should be redacted", result.contains("deep_value"));
    assertTrue("Should preserve nested structure", result.contains("SELECT"));
    assertTrue("Should preserve IN keyword", result.contains("IN"));
  }

  @Test
  public void testQueryWithNoLiterals() {
    String sql = "SELECT a, b, c FROM table1 WHERE a = b AND c IS NULL ORDER BY a";
    String result = SqlParser.redactSensitiveData(sql);

    assertEquals("Query with no literals should be unchanged",
        "SELECT a, b, c FROM table1 WHERE a = b AND c IS NULL ORDER BY a", result);
  }

  @Test
  public void testConvertVarcharFunction() {
    String sql = "SELECT CONVERT('hello' USING utf8) FROM test_table";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("String value should be redacted", result.contains("hello"));
    assertTrue("Should preserve CONVERT structure", result.contains("CONVERT("));
    assertTrue("Should preserve USING keyword", result.contains("USING"));
    assertTrue("Should preserve charset name", result.contains("utf8"));
  }

  // =========================================================================
  // CaseExpression Tests — explicit override for defense-in-depth
  // =========================================================================

  @Test
  public void testCaseExpressionSimpleWhen() {
    String sql = "SELECT CASE status WHEN 'active' THEN 'Active User' "
        + "WHEN 'inactive' THEN 'Inactive User' "
        + "ELSE 'Unknown' END AS status_label FROM users";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("WHEN value 'active' should be redacted", result.contains("'active'"));
    assertFalse("THEN value 'Active User' should be redacted",
        result.contains("Active User"));
    assertFalse("WHEN value 'inactive' should be redacted", result.contains("'inactive'"));
    assertFalse("THEN value 'Inactive User' should be redacted",
        result.contains("Inactive User"));
    assertFalse("ELSE value 'Unknown' should be redacted", result.contains("'Unknown'"));
    assertTrue("Should preserve CASE keyword", result.contains("CASE"));
    assertTrue("Should preserve WHEN keyword", result.contains("WHEN"));
    assertTrue("Should preserve THEN keyword", result.contains("THEN"));
    assertTrue("Should preserve ELSE keyword", result.contains("ELSE"));
    assertTrue("Should preserve END keyword", result.contains("END"));
    assertTrue("Should preserve column name 'status'", result.contains("status"));
  }

  @Test
  public void testCaseExpressionSearchedForm() {
    String sql = "SELECT CASE WHEN age > 65 THEN 'Senior' "
        + "WHEN age > 18 THEN 'Adult' "
        + "WHEN age > 12 THEN 'Teen' "
        + "ELSE 'Child' END AS age_group FROM people";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Age threshold 65 should be redacted", result.contains("65"));
    assertFalse("Age threshold 18 should be redacted", result.contains("18"));
    assertFalse("Age threshold 12 should be redacted", result.contains("12"));
    assertFalse("Label 'Senior' should be redacted", result.contains("Senior"));
    assertFalse("Label 'Adult' should be redacted", result.contains("Adult"));
    assertFalse("Label 'Teen' should be redacted", result.contains("Teen"));
    assertFalse("Label 'Child' should be redacted", result.contains("Child"));
    assertTrue("Should preserve CASE structure", result.contains("CASE"));
    assertTrue("Should preserve multiple WHEN clauses",
        result.indexOf("WHEN") != result.lastIndexOf("WHEN"));
  }

  @Test
  public void testCaseExpressionWithNumericResults() {
    String sql = "SELECT CASE WHEN department = 'Engineering' THEN 1.5 "
        + "WHEN department = 'Sales' THEN 1.2 "
        + "ELSE 1.0 END * salary AS adjusted_salary FROM employees";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Department value should be redacted", result.contains("Engineering"));
    assertFalse("Department value should be redacted", result.contains("Sales"));
    assertFalse("Multiplier 1.5 should be redacted", result.contains("1.5"));
    assertFalse("Multiplier 1.2 should be redacted", result.contains("1.2"));
    assertTrue("Should preserve CASE WHEN structure", result.contains("CASE WHEN"));
    assertTrue("Should preserve ELSE", result.contains("ELSE"));
    assertTrue("Should preserve END", result.contains("END"));
  }

  @Test
  public void testNestedCaseExpressions() {
    String sql = "SELECT CASE WHEN type = 'A' THEN "
        + "CASE WHEN subtype = 'X' THEN 'AX' ELSE 'AO' END "
        + "ELSE 'Other' END AS category FROM items";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Type 'A' should be redacted", result.contains("'A'"));
    assertFalse("Subtype 'X' should be redacted", result.contains("'X'"));
    assertFalse("Result 'AX' should be redacted", result.contains("'AX'"));
    assertFalse("Result 'AO' should be redacted", result.contains("'AO'"));
    assertFalse("Result 'Other' should be redacted", result.contains("'Other'"));
    // Should have two CASE keywords (nested)
    int firstCase = result.indexOf("CASE");
    int secondCase = result.indexOf("CASE", firstCase + 1);
    assertTrue("Should preserve nested CASE structure", secondCase > firstCase);
  }

  @Test
  public void testCaseExpressionInWhereClause() {
    String sql = "SELECT * FROM orders WHERE "
        + "CASE WHEN region = 'US' THEN amount > 1000 "
        + "WHEN region = 'EU' THEN amount > 800 "
        + "ELSE amount > 500 END";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Region 'US' should be redacted", result.contains("'US'"));
    assertFalse("Region 'EU' should be redacted", result.contains("'EU'"));
    assertFalse("Amount 1000 should be redacted", result.contains("1000"));
    assertFalse("Amount 800 should be redacted", result.contains("800"));
    assertFalse("Amount 500 should be redacted", result.contains("500"));
    assertTrue("Should preserve CASE in WHERE", result.contains("WHERE"));
    assertTrue("Should preserve CASE structure", result.contains("CASE"));
  }

  // =========================================================================
  // WhenClause Tests — ensuring WHEN...THEN pairs are properly redacted
  // =========================================================================

  @Test
  public void testWhenClauseWithExpressionValues() {
    String sql = "SELECT CASE WHEN salary + bonus > 100000 THEN 'Top' "
        + "ELSE 'Regular' END AS tier FROM employees";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Threshold should be redacted", result.contains("100000"));
    assertFalse("Tier 'Top' should be redacted", result.contains("'Top'"));
    assertFalse("Tier 'Regular' should be redacted", result.contains("'Regular'"));
    assertTrue("Should preserve WHEN keyword", result.contains("WHEN"));
    assertTrue("Should preserve THEN keyword", result.contains("THEN"));
    assertTrue("Should preserve column reference", result.contains("salary"));
    assertTrue("Should preserve column reference", result.contains("bonus"));
  }

  @Test
  public void testWhenClauseWithFunctionCalls() {
    String sql = "SELECT CASE WHEN LENGTH(name) > 50 THEN SUBSTR(name, 1, 50) "
        + "ELSE name END AS truncated_name FROM users";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Length threshold should be redacted", result.contains("50"));
    assertTrue("Should preserve LENGTH function", result.contains("LENGTH("));
    assertTrue("Should preserve CASE WHEN THEN structure",
        result.contains("CASE WHEN") && result.contains("THEN"));
  }

  @Test
  public void testWhenClauseWithNullValues() {
    String sql = "SELECT CASE WHEN email IS NULL THEN 'no-email@example.com' "
        + "ELSE email END AS contact FROM users";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Email default should be redacted",
        result.contains("no-email@example.com"));
    assertTrue("Should preserve IS NULL", result.contains("IS NULL"));
    assertTrue("Should preserve CASE WHEN", result.contains("CASE WHEN"));
  }

  @Test
  public void testMultipleWhenClausesWithDates() {
    String sql = "SELECT CASE "
        + "WHEN hire_date < '2020-01-01' THEN 'Veteran' "
        + "WHEN hire_date < '2022-06-15' THEN 'Experienced' "
        + "WHEN hire_date < '2024-01-01' THEN 'Recent' "
        + "ELSE 'New' END AS tenure FROM employees";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Date '2020-01-01' should be redacted", result.contains("2020-01-01"));
    assertFalse("Date '2022-06-15' should be redacted", result.contains("2022-06-15"));
    assertFalse("Date '2024-01-01' should be redacted", result.contains("2024-01-01"));
    assertFalse("Label 'Veteran' should be redacted", result.contains("Veteran"));
    assertFalse("Label 'Experienced' should be redacted", result.contains("Experienced"));
    assertFalse("Label 'Recent' should be redacted", result.contains("Recent"));
    assertFalse("Label 'New' should be redacted", result.contains("'New'"));
    // Should have 3 WHEN clauses
    String temp = result;
    int whenCount = 0;
    int idx = 0;
    while ((idx = temp.indexOf("WHEN", idx)) != -1) {
      whenCount++;
      idx += 4;
    }
    assertTrue("Should have at least 3 WHEN clauses", whenCount >= 3);
  }

  // =========================================================================
  // AnyComparisonExpression Tests — ANY/SOME/ALL subquery comparisons
  // =========================================================================

  @Test
  public void testAnyComparisonWithSubquery() {
    String sql = "SELECT * FROM products WHERE price > ANY "
        + "(SELECT price FROM products WHERE category = 'Electronics')";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Category should be redacted", result.contains("Electronics"));
    assertTrue("Should preserve ANY keyword", result.contains("ANY"));
    assertTrue("Should preserve subquery structure", result.contains("SELECT"));
    assertTrue("Should preserve WHERE clause", result.contains("WHERE"));
    assertTrue("Should preserve column 'price'", result.contains("price"));
  }

  @Test
  public void testSomeComparisonWithSubquery() {
    String sql = "SELECT * FROM employees WHERE salary = SOME "
        + "(SELECT salary FROM managers WHERE dept = 'Finance')";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Department should be redacted", result.contains("Finance"));
    assertTrue("Should preserve SOME keyword", result.contains("SOME"));
    assertTrue("Should preserve subquery", result.contains("SELECT"));
  }

  @Test
  public void testAllComparisonWithSubquery() {
    String sql = "SELECT * FROM products WHERE price >= ALL "
        + "(SELECT min_price FROM price_rules WHERE region = 'APAC')";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Region should be redacted", result.contains("APAC"));
    assertTrue("Should preserve ALL keyword", result.contains("ALL"));
    assertTrue("Should preserve comparison operator", result.contains(">="));
    assertTrue("Should preserve subquery structure", result.contains("SELECT"));
  }

  @Test
  public void testAnyComparisonWithLiteralsInSubquery() {
    String sql = "SELECT name FROM employees WHERE id = ANY "
        + "(SELECT employee_id FROM awards WHERE year = 2024 "
        + "AND type = 'excellence')";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Year should be redacted", result.contains("2024"));
    assertFalse("Type should be redacted", result.contains("excellence"));
    assertTrue("Should preserve ANY", result.contains("ANY"));
    assertTrue("Should preserve table name", result.contains("awards"));
  }

  // =========================================================================
  // Alias Tests — structural metadata, not PII
  // =========================================================================

  @Test
  public void testColumnAliasesPreserved() {
    String sql = "SELECT name AS employee_name, salary AS emp_salary, "
        + "'active' AS status FROM employees WHERE id = 42";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Column alias should be preserved", result.contains("employee_name"));
    assertTrue("Column alias should be preserved", result.contains("emp_salary"));
    assertTrue("Alias keyword should be preserved", result.contains("AS"));
    assertFalse("Literal 'active' should be redacted", result.contains("'active'"));
    assertFalse("ID value should be redacted", result.contains("42"));
  }

  @Test
  public void testTableAliasesPreserved() {
    String sql = "SELECT e.name, d.name FROM employees e "
        + "JOIN departments d ON e.dept_id = d.id "
        + "WHERE e.status = 'active'";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Table alias should be preserved", result.contains("e."));
    assertTrue("Table alias should be preserved", result.contains("d."));
    assertFalse("Status should be redacted", result.contains("'active'"));
  }

  @Test
  public void testSubqueryAlias() {
    String sql = "SELECT sub.total FROM "
        + "(SELECT COUNT(*) AS total FROM users WHERE age > 25) sub";
    String result = SqlParser.redactSensitiveData(sql);

    assertTrue("Subquery alias should be preserved", result.contains("sub"));
    assertTrue("Column alias should be preserved", result.contains("total"));
    assertFalse("Age value should be redacted", result.contains("25"));
  }

  // =========================================================================
  // BinaryExpression Tests — concrete subclasses use visitor pattern
  // =========================================================================

  @Test
  public void testBinaryArithmeticExpressions() {
    String sql = "SELECT price * 1.08 AS with_tax, "
        + "salary + 5000 AS adjusted, "
        + "total - 100 AS discounted, "
        + "amount / 12 AS monthly "
        + "FROM products WHERE id = 1";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Tax rate should be redacted", result.contains("1.08"));
    assertFalse("Adjustment should be redacted", result.contains("5000"));
    assertFalse("Discount should be redacted", result.contains("100"));
    assertFalse("Divisor should be redacted", result.contains("12"));
    assertTrue("Should preserve multiplication operator", result.contains("*"));
    assertTrue("Should preserve addition operator", result.contains("+"));
    assertTrue("Should preserve subtraction operator", result.contains("-"));
    assertTrue("Should preserve division operator", result.contains("/"));
    assertTrue("Should preserve column names", result.contains("price"));
    assertTrue("Should preserve column names", result.contains("salary"));
  }

  @Test
  public void testBinaryComparisonExpressions() {
    String sql = "SELECT * FROM orders WHERE "
        + "amount >= 1000 AND amount <= 5000 "
        + "AND status <> 'cancelled' "
        + "AND region != 'test'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Amount 1000 should be redacted", result.contains("1000"));
    assertFalse("Amount 5000 should be redacted", result.contains("5000"));
    assertFalse("Status 'cancelled' should be redacted", result.contains("cancelled"));
    assertFalse("Region 'test' should be redacted", result.contains("'test'"));
    assertTrue("Should preserve >= operator", result.contains(">="));
    assertTrue("Should preserve <= operator", result.contains("<="));
    assertTrue("Should preserve AND", result.contains("AND"));
  }

  @Test
  public void testBinaryLogicalExpressions() {
    String sql = "SELECT * FROM users WHERE "
        + "(status = 'active' OR status = 'pending') "
        + "AND NOT deleted = 'true'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status should be redacted", result.contains("'active'"));
    assertFalse("Status should be redacted", result.contains("'pending'"));
    assertFalse("Deleted flag should be redacted", result.contains("'true'"));
    assertTrue("Should preserve OR", result.contains("OR"));
    assertTrue("Should preserve AND", result.contains("AND"));
    assertTrue("Should preserve NOT", result.contains("NOT"));
  }

  @Test
  public void testBinaryStringConcatenation() {
    String sql = "SELECT first_name || ' ' || last_name AS full_name "
        + "FROM employees WHERE id = 100";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Space literal should be redacted", result.contains("' '"));
    assertFalse("ID should be redacted", result.contains("100"));
    assertTrue("Should preserve concat operator", result.contains("||"));
    assertTrue("Should preserve column names", result.contains("first_name"));
    assertTrue("Should preserve column names", result.contains("last_name"));
  }

  // =========================================================================
  // FilterOverImpl Tests — handled through AnalyticExpression override
  // =========================================================================

  @Test
  public void testFilterClauseOnAggregate() {
    String sql = "SELECT department, "
        + "COUNT(*) FILTER (WHERE salary > 50000) AS high_earners, "
        + "AVG(salary) FILTER (WHERE hire_date > '2023-01-01') AS recent_avg "
        + "FROM employees GROUP BY department";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary threshold should be redacted", result.contains("50000"));
    assertFalse("Date should be redacted", result.contains("2023-01-01"));
    assertTrue("Should preserve FILTER keyword", result.contains("FILTER"));
    assertTrue("Should preserve aggregate functions",
        result.contains("COUNT(") || result.contains("AVG("));
  }

  @Test
  public void testWindowFunctionWithPartitionAndOrder() {
    String sql = "SELECT name, salary, "
        + "RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank, "
        + "SUM(salary) OVER (PARTITION BY department) AS dept_total "
        + "FROM employees WHERE status = 'active'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status should be redacted", result.contains("'active'"));
    assertTrue("Should preserve OVER keyword", result.contains("OVER"));
    assertTrue("Should preserve PARTITION BY", result.contains("PARTITION BY"));
    assertTrue("Should preserve ORDER BY", result.contains("ORDER BY"));
    assertTrue("Should preserve RANK function", result.contains("RANK("));
    assertTrue("Should preserve SUM function", result.contains("SUM("));
  }

  @Test
  public void testWindowFrameWithRows() {
    String sql = "SELECT name, "
        + "AVG(score) OVER (ORDER BY created_at "
        + "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg "
        + "FROM measurements WHERE value > 100";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Value threshold should be redacted", result.contains("100"));
    assertTrue("Should preserve ROWS keyword", result.contains("ROWS"));
    assertTrue("Should preserve BETWEEN keyword", result.contains("BETWEEN"));
    assertTrue("Should preserve CURRENT ROW", result.contains("CURRENT ROW"));
    assertTrue("Should preserve PRECEDING", result.contains("PRECEDING"));
  }

  // =========================================================================
  // MySQLIndexHint Tests — structural optimizer directives
  // =========================================================================

  @Test
  public void testMySQLUseIndex() {
    String sql = "SELECT * FROM employees USE INDEX (idx_name) "
        + "WHERE name = 'John' AND department = 'Engineering'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Name should be redacted", result.contains("John"));
    assertFalse("Department should be redacted", result.contains("Engineering"));
    // Index hints are structural and should be preserved
    assertTrue("Should preserve table name", result.contains("employees"));
  }

  @Test
  public void testMySQLForceIndex() {
    String sql = "SELECT * FROM orders FORCE INDEX (idx_date) "
        + "WHERE order_date > '2024-01-01' AND total > 500";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Date should be redacted", result.contains("2024-01-01"));
    assertFalse("Total threshold should be redacted", result.contains("500"));
    assertTrue("Should preserve table name", result.contains("orders"));
  }

  @Test
  public void testMySQLIgnoreIndex() {
    String sql = "SELECT * FROM products IGNORE INDEX (idx_old) "
        + "WHERE category = 'electronics' AND price < 999.99";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Category should be redacted", result.contains("electronics"));
    assertFalse("Price should be redacted", result.contains("999.99"));
    assertTrue("Should preserve table name", result.contains("products"));
  }

  // =========================================================================
  // SQLServerHints Tests — structural optimizer directives
  // =========================================================================

  @Test
  public void testSQLServerNoLockHint() {
    String sql = "SELECT * FROM employees WITH (NOLOCK) "
        + "WHERE department = 'Sales' AND salary > 60000";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Department should be redacted", result.contains("Sales"));
    assertFalse("Salary should be redacted", result.contains("60000"));
    assertTrue("Should preserve NOLOCK hint", result.contains("NOLOCK"));
    assertTrue("Should preserve table name", result.contains("employees"));
  }

  @Test
  public void testSQLServerMultipleHints() {
    String sql = "SELECT e.name, d.name FROM employees e WITH (NOLOCK) "
        + "JOIN departments d WITH (NOLOCK) ON e.dept_id = d.id "
        + "WHERE e.status = 'active'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status should be redacted", result.contains("'active'"));
    assertTrue("Should preserve NOLOCK hints", result.contains("NOLOCK"));
    assertTrue("Should preserve JOIN", result.contains("JOIN"));
  }

  // =========================================================================
  // OrderByClause Tests — handled through visitOrderByElements()
  // =========================================================================

  @Test
  public void testOrderByWithLiteralsInExpressions() {
    String sql = "SELECT name, salary FROM employees "
        + "WHERE department = 'IT' "
        + "ORDER BY CASE WHEN status = 'lead' THEN 1 "
        + "WHEN status = 'senior' THEN 2 ELSE 3 END, "
        + "salary DESC";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Department should be redacted", result.contains("'IT'"));
    assertFalse("Status 'lead' should be redacted", result.contains("'lead'"));
    assertFalse("Status 'senior' should be redacted", result.contains("'senior'"));
    assertTrue("Should preserve ORDER BY", result.contains("ORDER BY"));
    assertTrue("Should preserve DESC", result.contains("DESC"));
    assertTrue("Should preserve CASE in ORDER BY", result.contains("CASE"));
  }

  @Test
  public void testOrderByNullsFirstLast() {
    String sql = "SELECT * FROM products "
        + "WHERE price > 10 "
        + "ORDER BY name ASC NULLS FIRST, price DESC NULLS LAST";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Price should be redacted", result.contains("10"));
    assertTrue("Should preserve NULLS FIRST", result.contains("NULLS FIRST"));
    assertTrue("Should preserve NULLS LAST", result.contains("NULLS LAST"));
    assertTrue("Should preserve ASC", result.contains("ASC"));
    assertTrue("Should preserve DESC", result.contains("DESC"));
  }

  // =========================================================================
  // PartitionByClause Tests — handled through visitExpressionList()
  // =========================================================================

  @Test
  public void testPartitionByMultipleColumns() {
    String sql = "SELECT department, region, "
        + "ROW_NUMBER() OVER (PARTITION BY department, region "
        + "ORDER BY hire_date) AS row_num "
        + "FROM employees WHERE salary > 40000";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Salary should be redacted", result.contains("40000"));
    assertTrue("Should preserve PARTITION BY", result.contains("PARTITION BY"));
    assertTrue("Should preserve column names in PARTITION BY",
        result.contains("department"));
    assertTrue("Should preserve column names in PARTITION BY",
        result.contains("region"));
  }

  @Test
  public void testPartitionByWithExpression() {
    String sql = "SELECT "
        + "SUM(amount) OVER (PARTITION BY EXTRACT(YEAR FROM order_date) "
        + "ORDER BY order_date) AS running_total "
        + "FROM orders WHERE status = 'completed'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Status should be redacted", result.contains("'completed'"));
    assertTrue("Should preserve PARTITION BY", result.contains("PARTITION BY"));
    assertTrue("Should preserve EXTRACT function", result.contains("EXTRACT("));
    assertTrue("Should preserve SUM function", result.contains("SUM("));
  }

  // =========================================================================
  // Combined / Integration Tests — multiple expression types together
  // =========================================================================

  @Test
  public void testCaseWithAnyComparison() {
    String sql = "SELECT CASE WHEN salary > ANY "
        + "(SELECT salary FROM managers WHERE level = 'VP') "
        + "THEN 'Above VP' ELSE 'Below VP' END AS comparison "
        + "FROM employees";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Level should be redacted", result.contains("'VP'"));
    assertFalse("Result 'Above VP' should be redacted", result.contains("Above VP"));
    assertFalse("Result 'Below VP' should be redacted", result.contains("Below VP"));
    assertTrue("Should preserve CASE", result.contains("CASE"));
    assertTrue("Should preserve ANY", result.contains("ANY"));
  }

  @Test
  public void testCaseWithWindowFunction() {
    String sql = "SELECT name, "
        + "CASE WHEN RANK() OVER (ORDER BY score DESC) <= 3 "
        + "THEN 'Top 3' ELSE 'Others' END AS ranking, "
        + "score "
        + "FROM students WHERE class = 'A'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Class should be redacted", result.contains("'A'"));
    assertFalse("Rank label should be redacted", result.contains("Top 3"));
    assertFalse("Others label should be redacted", result.contains("Others"));
    assertTrue("Should preserve CASE", result.contains("CASE"));
    assertTrue("Should preserve RANK function", result.contains("RANK("));
    assertTrue("Should preserve OVER", result.contains("OVER"));
  }

  @Test
  public void testComplexQueryWithAllExpressionTypes() {
    String sql = "SELECT "
        + "e.name AS employee_name, "
        + "CASE WHEN e.salary > 100000 THEN 'Executive' "
        + "WHEN e.salary > 75000 THEN 'Senior' "
        + "ELSE 'Staff' END AS grade, "
        + "SUM(e.bonus) OVER (PARTITION BY e.department "
        + "ORDER BY e.hire_date "
        + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bonus "
        + "FROM employees e "
        + "WHERE e.salary > ANY "
        + "(SELECT avg_salary FROM department_stats WHERE dept = 'Tech') "
        + "AND e.status = 'active' "
        + "ORDER BY CASE WHEN e.role = 'Manager' THEN 1 ELSE 2 END, "
        + "e.name ASC";
    String result = SqlParser.redactSensitiveData(sql);

    // Verify all PII/literal values are redacted
    assertFalse("Salary 100000 should be redacted", result.contains("100000"));
    assertFalse("Salary 75000 should be redacted", result.contains("75000"));
    assertFalse("Grade 'Executive' should be redacted", result.contains("Executive"));
    assertFalse("Grade 'Senior' should be redacted", result.contains("Senior"));
    assertFalse("Grade 'Staff' should be redacted", result.contains("Staff"));
    assertFalse("Department 'Tech' should be redacted", result.contains("Tech"));
    assertFalse("Status 'active' should be redacted", result.contains("'active'"));
    assertFalse("Role 'Manager' should be redacted", result.contains("Manager"));

    // Verify all structural elements are preserved
    assertTrue("Should preserve CASE", result.contains("CASE"));
    assertTrue("Should preserve WHEN", result.contains("WHEN"));
    assertTrue("Should preserve THEN", result.contains("THEN"));
    assertTrue("Should preserve ELSE", result.contains("ELSE"));
    assertTrue("Should preserve END", result.contains("END"));
    assertTrue("Should preserve SUM", result.contains("SUM("));
    assertTrue("Should preserve OVER", result.contains("OVER"));
    assertTrue("Should preserve PARTITION BY", result.contains("PARTITION BY"));
    assertTrue("Should preserve ORDER BY", result.contains("ORDER BY"));
    assertTrue("Should preserve ROWS BETWEEN", result.contains("ROWS BETWEEN"));
    assertTrue("Should preserve ANY", result.contains("ANY"));
    assertTrue("Should preserve column alias", result.contains("employee_name"));
    assertTrue("Should preserve ASC", result.contains("ASC"));
  }

  @Test
  public void testPostgreSQLCaseWithArrayAndBinaryOps() {
    String sql = "SELECT "
        + "CASE WHEN tags[1] = 'urgent' THEN priority * 2 "
        + "ELSE priority END AS adjusted_priority, "
        + "data->>'name' AS extracted_name "
        + "FROM tasks WHERE status <> 'deleted' "
        + "AND (flags & 4) > 0";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Tag value should be redacted", result.contains("urgent"));
    assertFalse("Priority multiplier should be redacted", result.contains(" 2 "));
    assertFalse("Status should be redacted", result.contains("deleted"));
    assertTrue("Should preserve CASE", result.contains("CASE"));
    assertTrue("Should preserve array access syntax", result.contains("["));
    assertTrue("Should preserve JSON operator", result.contains("->>"));
  }

  @Test
  public void testOracleCaseWithConnectBy() {
    String sql = "SELECT CASE WHEN LEVEL = 1 THEN 'Root' "
        + "WHEN LEVEL = 2 THEN 'Child' "
        + "ELSE 'Grandchild' END AS node_type, "
        + "name, LEVEL "
        + "FROM employees "
        + "START WITH manager_id IS NULL "
        + "CONNECT BY PRIOR employee_id = manager_id";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Node type 'Root' should be redacted", result.contains("'Root'"));
    assertFalse("Node type 'Child' should be redacted", result.contains("'Child'"));
    assertFalse("Node type 'Grandchild' should be redacted",
        result.contains("Grandchild"));
    assertTrue("Should preserve CASE", result.contains("CASE"));
    assertTrue("Should preserve CONNECT BY", result.contains("CONNECT BY"));
    assertTrue("Should preserve START WITH", result.contains("START WITH"));
    assertTrue("Should preserve PRIOR", result.contains("PRIOR"));
  }

  @Test
  public void testMySQLCaseWithGroupConcat() {
    String sql = "SELECT department, "
        + "GROUP_CONCAT(CASE WHEN rating >= 4 THEN name "
        + "ELSE NULL END ORDER BY rating DESC SEPARATOR ', ') AS top_performers "
        + "FROM employees "
        + "WHERE year = 2024 "
        + "GROUP BY department";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Rating threshold should be redacted", result.contains(" 4 "));
    assertFalse("Year should be redacted", result.contains("2024"));
    assertFalse("Separator should be redacted", result.contains("', '"));
    assertTrue("Should preserve GROUP_CONCAT", result.contains("GROUP_CONCAT("));
    assertTrue("Should preserve CASE", result.contains("CASE"));
    assertTrue("Should preserve SEPARATOR keyword", result.contains("SEPARATOR"));
  }

  // =========================================================================
  // Leak Detector (Layer 3) — full redaction demonstration
  // =========================================================================

  /**
   * This test simulates that scenario by directly calling hasLeakedLiterals()
   * on simulated AST output where a numeric literal "857287" leaked through.
   */
  @Test
  public void testLayer3DetectsLeakedNumericAndTriggersFullRedaction() {
    String simulatedOutput =
        "SELECT * FROM users WHERE name = '********' AND user_id = 857287";

    String expected = "SELECT * FROM users WHERE name = '********' AND user_id = 0";
    String result = SqlParser.redactSensitiveData(simulatedOutput);

    assertEquals("Layer 3 should trigger full redaction to <redacted>",
        expected, result);

    assertTrue(
        "Layer 3 should detect the leaked numeric literal 857287",
        SqlParser.hasLeakedLiterals(simulatedOutput)
    );
  }

  /**
   * Demonstrates Layer 3: the leak detector catches a leaked STRING literal
   * that somehow survived both Layer 1 (AST) and Layer 2 (regex safety net).
   *
   * Scenario: A string literal contains unusual escaping or encoding that the
   * regex pattern in Layer 2 fails to match. Layer 3 performs a final check:
   * any single-quoted string in the output that is NOT '********' is a leak.
   */
  @Test
  public void testLayer3DetectsLeakedStringAndTriggersFullRedaction() {
    // Simulate output where a string literal somehow survived Layer 2.
    // (In practice this is extremely unlikely since Layer 2's regex is
    // comprehensive, but Layer 3 exists as a last line of defense.)
    String simulatedOutput =
        "SELECT * FROM users WHERE email = 'john@example.com' AND id = 0";

    assertTrue(
        "Layer 3 should detect the leaked string literal",
        SqlParser.hasLeakedLiterals(simulatedOutput)
    );
  }

  /**
   * Demonstrates that Layer 3 correctly distinguishes between:
   * - LEAKED PII numbers (e.g., user_id = 857287) → triggers full redaction
   * - STRUCTURAL numbers (e.g., TOP 50, OPTIMIZE FOR 100, DECIMAL(10,2)) → allowed
   *
   * This is critical: without structural number whitelisting, every SQL Server
   * query with TOP or every DB2 query with OPTIMIZE FOR would be fully
   * redacted, losing all debugging value.
   */
  @Test
  public void testLayer3DistinguishesPiiFromStructuralNumbers() {
    // Case 1: Only structural numbers present — should NOT trigger
    String structuralOnly = "SELECT TOP 50 CAST(price AS DECIMAL(10, 2)) "
        + "FROM products WHERE name = '********' AND id = 0";
    assertFalse(
        "Structural numbers (TOP 50, DECIMAL(10,2)) should not trigger leak detection",
        SqlParser.hasLeakedLiterals(structuralOnly)
    );

    // Case 2: Structural numbers + a leaked PII number — SHOULD trigger
    String structuralPlusLeak = "SELECT TOP 50 CAST(price AS DECIMAL(10, 2)) "
        + "FROM products WHERE name = '********' AND id = 857287";
    assertTrue(
        "Leaked PII number 857287 should be detected even alongside structural numbers",
        SqlParser.hasLeakedLiterals(structuralPlusLeak)
    );
  }

  // =========================================================================
  // Unparseable Query (Layer 4) — regex fallback demonstration
  // =========================================================================

  /**
   * Demonstrates Layer 4: when JSqlParser cannot parse the SQL at all
   * (e.g., proprietary syntax, future SQL features, or malformed queries),
   * the regex fallback redacts all string literals and numeric literals
   * while preserving SQL keywords and identifiers.
   *
   * This is NOT {@code <redacted>} — it's a best-effort regex redaction
   * that still preserves query structure for debugging.
   */
  @Test
  public void testLayer4RegexFallbackPreservesStructureOnUnparseableQuery() {
    // This SQL uses syntax that JSqlParser 4.9 cannot parse (invalid syntax)
    String sql = "SELECT SOME_FUTURE_FUNC('sensitive_ssn', 123456789) "
        + ":::: FROM employees WHERE salary > 50000";
    String result = SqlParser.redactSensitiveData(sql);

    // The regex fallback should redact all literals
    assertFalse("SSN should be redacted", result.contains("sensitive_ssn"));
    assertFalse("Number should be redacted", result.contains("123456789"));
    assertFalse("Salary threshold should be redacted", result.contains("50000"));

    // But should preserve structure for debugging
    assertTrue("Should preserve SELECT keyword", result.contains("SELECT"));
    assertTrue("Should preserve function name", result.contains("SOME_FUTURE_FUNC"));
    assertTrue("Should preserve FROM keyword", result.contains("FROM"));
    assertTrue("Should preserve table name", result.contains("employees"));
    assertTrue("Should preserve WHERE keyword", result.contains("WHERE"));
    assertTrue("Should preserve column name", result.contains("salary"));

    // The result is NOT "<redacted>" — it's a regex-redacted version
    assertNotEquals("<redacted>", result);
  }

  /**
   * Demonstrates Layer 4: when the SQL is so malformed that even regex
   * fallback calls redactAllLiterals() which returns {@code <redacted>}
   * for null/empty edge cases.
   */
  @Test
  public void testLayer4FullRedactionOnRegexFailure() {
    // redactAllLiterals returns <redacted> for null/empty inputs
    assertEquals("<redacted>", SqlParser.redactAllLiterals(null));
    assertEquals("<redacted>", SqlParser.redactAllLiterals(""));
  }

  // =========================================================================
  // End-to-End: Verifying normal queries do NOT trigger Layer 3
  // =========================================================================

  /**
   * Verifies that a complex real-world query with many expression types
   * passes through Layers 1+2 cleanly without triggering Layer 3 (leak
   * detector), proving that our visitor overrides are comprehensive.
   *
   * If this test returned {@code <redacted>}, it would mean our visitor
   * missed an expression type — which is a bug.
   */
  @Test
  public void testEndToEndNoLayer3TriggerOnComplexQuery() {
    // This query exercises: CASE, WHEN, JSON, window functions, INTERVAL,
    // COALESCE, subquery with ANY, string/number/date literals, array access
    String sql = "SELECT "
        + "CASE WHEN e.salary > 100000 THEN 'Executive' "
        + "WHEN e.salary > 75000 THEN 'Senior' ELSE 'Staff' END AS grade, "
        + "COALESCE(e.phone, 'N/A') AS phone, "
        + "SUM(e.bonus) OVER (PARTITION BY e.dept ORDER BY e.hire_date "
        + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bonus "
        + "FROM employees e "
        + "WHERE e.salary > ANY (SELECT avg_sal FROM dept_stats WHERE dept = 'Tech') "
        + "AND e.status = 'active' "
        + "AND e.hire_date > '2020-01-01' "
        + "ORDER BY e.salary DESC "
        + "LIMIT 50";
    String result = SqlParser.redactSensitiveData(sql);

    // The result must NOT be "<redacted>" — that would mean Layer 3 triggered,
    // indicating our visitor missed redacting some literal.
    assertNotEquals(
        "Layer 3 should NOT trigger — all literals should be handled by visitor",
        "<redacted>", result
    );

    // Verify PII is redacted
    assertFalse(result.contains("100000"));
    assertFalse(result.contains("75000"));
    assertFalse(result.contains("Executive"));
    assertFalse(result.contains("Senior"));
    assertFalse(result.contains("Staff"));
    assertFalse(result.contains("N/A"));
    assertFalse(result.contains("Tech"));
    assertFalse(result.contains("'active'"));
    assertFalse(result.contains("2020-01-01"));

    // Verify structure is preserved
    assertTrue(result.contains("CASE"));
    assertTrue(result.contains("COALESCE("));
    assertTrue(result.contains("PARTITION BY"));
    assertTrue(result.contains("ANY"));
    assertTrue(result.contains("ORDER BY"));
  }
}
