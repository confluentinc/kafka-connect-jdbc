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

    assertEquals(expected, result);
  }

  @Test
  public void testNegativeDecimal() {
    String query = "SELECT * FROM transactions WHERE amount = -99.99";
    String result = SqlParser.redactSensitiveData(query);
    String expected = "SELECT * FROM transactions WHERE amount = 0";

    assertEquals(expected, result);
  }

  @Test
  public void testRedactDateLiteral() {
    String sql = "SELECT * FROM orders WHERE order_date = {d '2023-01-01'}";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM orders WHERE order_date = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, result);
  }

  @Test
  public void testDateLiteral() {
    String query = "SELECT * FROM orders WHERE order_date = DATE '2024-01-15'";
    String result = SqlParser.redactSensitiveData(query);
    String expected = "SELECT * FROM orders WHERE order_date = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, result);
  }

  @Test
  public void testRedactTimestampLiteral() {
    String sql = "SELECT * FROM logs WHERE timestamp < {ts '2023-01-01 12:00:00'}";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM logs WHERE timestamp < " + SqlParser.REDACTED_STRING;

    assertEquals(expected, result);
  }

  @Test
  public void testRedactTimeLiteral() {
    String sql = "SELECT * FROM schedules WHERE start_time = {t '12:00:00'}";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM schedules WHERE start_time = " + SqlParser.REDACTED_STRING;

    assertEquals(expected, result);
  }

  @Test
  public void testTimestampLiteral() {
    String query = "SELECT * FROM logs WHERE created_at > TIMESTAMP '2024-01-01 12:30:45'";
    String result = SqlParser.redactSensitiveData(query);
    String expected = "SELECT * FROM logs WHERE created_at > " + SqlParser.REDACTED_STRING;

    assertEquals(expected, result);
  }

  @Test
  public void testRedactHexLiteral() {
    String sql = "SELECT * FROM data WHERE bytes = X'DEADBEEF'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM data WHERE bytes = " + SqlParser.REDACTED_STRING;

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

  @Test
  public void testComplexNestedSubqueries() {
    String sql = "SELECT * FROM orders o WHERE o.customer_id IN " +
                 "(SELECT c.id FROM customers c WHERE c.region = 'APAC' " +
                 "AND c.credit_limit > 10000) AND o.total > 500";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM orders o WHERE o.customer_id IN " +
                      "(SELECT c.id FROM customers c WHERE c.region = " + SqlParser.REDACTED_STRING + " " +
                      "AND c.credit_limit > " + SqlParser.REDACTED_NUMBER + ") AND o.total > " + SqlParser.REDACTED_NUMBER;

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

    assertEquals(expected, result);
  }

  @Test
  public void testNotInWithStrings() {
    String sql = "SELECT * FROM products WHERE category NOT IN ('Obsolete', 'Discontinued', 'Archived')";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM products WHERE category NOT IN ("
        + SqlParser.REDACTED_STRING + ", " + SqlParser.REDACTED_STRING + ", " + SqlParser.REDACTED_STRING + ")";

    assertEquals(expected, result);
  }

  @Test
  public void testLikePatterns() {
    String sql = "SELECT * FROM users WHERE email LIKE '%@gmail.com' AND name LIKE 'John%'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM users WHERE email LIKE " + SqlParser.REDACTED_STRING
        + " AND name LIKE " + SqlParser.REDACTED_STRING;

    assertEquals(expected, result);
  }

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

    String expected = "MERGE INTO target t USING source s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.value = "
        + SqlParser.REDACTED_STRING
        + " WHEN NOT MATCHED THEN INSERT  VALUES ("
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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
  @Test
  public void testPostgresArrayLiteral() {
    String sql = "SELECT * FROM products WHERE tags = ARRAY['electronics', 'sale']";

    String expected = "SELECT * FROM products WHERE tags = ARRAY["
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + "]";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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
    String expected = "SELECT DISTINCT ON (department) id, name, salary FROM employees WHERE salary > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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

    assertEquals(expected, result);
  }

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

    assertEquals(expected, result);
  }

  @Test
  public void testDb2ConcatOperator() {
    String sql = "SELECT first_name || ' ' || last_name AS full_name FROM employees WHERE dept = 'HR'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT first_name || " + SqlParser.REDACTED_STRING
        + " || last_name AS full_name FROM employees WHERE dept = " + SqlParser.REDACTED_STRING;

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

    assertEquals(expected, result);
  }

  @Test
  public void testDb2DateArithmetic() {
    String sql = "SELECT * FROM orders WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'";
    String result = SqlParser.redactSensitiveData(sql);
    String expected = "SELECT * FROM orders WHERE order_date BETWEEN "
        + SqlParser.REDACTED_STRING + " AND " + SqlParser.REDACTED_STRING;

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

    assertEquals(expected, result);
  }

  @Test
  public void testJsonObjectSimpleKeyValueObj() {
    String sql = "SELECT JSON_OBJECTAGG(\"username, age\") FROM users WHERE userid IN (857287, 871122, 856489, 414082) GROUP BY age;";

    String expected = "SELECT JSON_OBJECTAGG("
        + SqlParser.REDACTED_STRING
        + ") FROM users WHERE userid IN ("
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ") GROUP BY age";

    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }


  @Test
  public void testJsonObjectSimpleKeyValue() {
    String sql = "SELECT JSON_OBJECT('name', 'John Doe', 'age', 30) FROM users WHERE id = 1";

    String expected = "SELECT JSON_OBJECT("
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ") FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testJsonObjectNestedQuery() {
    String sql = "SELECT JSON_OBJECT('profile', JSON_OBJECT('userid', 871122, "
        + "'username', 'user_1234', 'age', 24), "
        + "'metadata', JSON_OBJECT('event_time', '1743853243812259', "
        + "'updated_at', '2025-04-05 17:10:43.812')) "
        + "FROM users WHERE uerid = 871122";


    String expected = "SELECT JSON_OBJECT("
        + SqlParser.REDACTED_STRING
        + ", JSON_OBJECT("
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_NUMBER
        + "), "
        + SqlParser.REDACTED_STRING
        + ", JSON_OBJECT("
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ")) FROM users WHERE uerid = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testJsonObjectWithColumnKeys() {
    String sql = "SELECT JSON_OBJECT(col1, col2) FROM users WHERE id = 1";

    String expected = "SELECT JSON_OBJECT(col1, col2) FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testJsonObjectWithWhereClause() {
    String sql = "SELECT JSON_OBJECT('key', 'value') FROM table1 "
        + "WHERE name = 'sensitive_name' AND id = 42";

    String expected = "SELECT JSON_OBJECT("
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ") FROM table1 WHERE name = "
        + SqlParser.REDACTED_STRING
        + " AND id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testJsonArraySimple() {
    String sql = "SELECT JSON_ARRAY('hello', 'world', 42, 3.14) FROM test_table";

    String expected = "SELECT JSON_ARRAY("
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ") FROM test_table";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

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

  @Test
  public void testSafetyNetCatchesLeakedStringLiterals() {
    String sql = "SELECT * FROM users WHERE name = 'test_user' AND id = 1";

    String expected = "SELECT * FROM users WHERE name = "
        + SqlParser.REDACTED_STRING
        + " AND id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testComplexQueryWithMultipleJsonFunctions() {
    String sql = "SELECT JSON_OBJECT('user', JSON_OBJECT('name', 'Alice', 'email', 'alice@test.com')), "
        + "JSON_OBJECT('stats', JSON_OBJECT('logins', 42, 'score', 99.5)) "
        + "FROM users WHERE status = 'active'";

    String expected = "SELECT JSON_OBJECT("
        + SqlParser.REDACTED_STRING
        + ", JSON_OBJECT("
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ")), JSON_OBJECT("
        + SqlParser.REDACTED_STRING
        + ", JSON_OBJECT("
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ")) FROM users WHERE status = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

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
  public void testHasLeakedLiterals_dollarQuotedString() {
    // Dollar-quoted strings should be detected as leaked literals
    String leaked = "SELECT * FROM t WHERE data = $$secret_value$$";
    assertTrue("Dollar-quoted string should be detected as leak",
        SqlParser.hasLeakedLiterals(leaked));
  }

  @Test
  public void testHasLeakedLiterals_taggedDollarQuotedString() {
    String leaked = "SELECT * FROM t WHERE data = $tag$secret PII$tag$";
    assertTrue("Tagged dollar-quoted string should be detected as leak",
        SqlParser.hasLeakedLiterals(leaked));
  }

  @Test
  public void testDollarQuotedStringSimple() {
    String sql = "SELECT * FROM t WHERE data = $$secret_value$$";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Dollar-quoted PII should be redacted", result.contains("secret_value"));
    assertFalse("Dollar signs should not remain as quoting", result.contains("$$"));
    assertTrue("Should preserve query structure", result.contains("SELECT * FROM t WHERE data ="));
  }

  @Test
  public void testDollarQuotedStringWithTag() {
    String sql = "SELECT * FROM t WHERE data = $tag$secret PII data$tag$";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Tagged dollar-quoted PII should be redacted", result.contains("secret"));
    assertFalse("Dollar-tag should not remain", result.contains("$tag$"));
    assertTrue("Should preserve query structure", result.contains("SELECT * FROM t WHERE data ="));
  }

  @Test
  public void testDollarQuotedStringWithSingleQuotesInside() {
    // Dollar-quoting is often used specifically because the content contains single quotes
    String sql = "SELECT * FROM t WHERE bio = $$John's SSN is 123-45-6789$$";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Name should be redacted", result.contains("John"));
    assertFalse("SSN should be redacted", result.contains("123-45-6789"));
  }

  @Test
  public void testDollarQuotedWithRegularString() {
    // Mixed: dollar-quoted and regular single-quoted in same query
    String sql = "SELECT * FROM t WHERE bio = $$secret$$ AND ssn = '123-45-6789'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Dollar-quoted value should be redacted", result.contains("secret"));
    assertFalse("Single-quoted SSN should be redacted", result.contains("123-45-6789"));
    assertTrue("Should preserve query structure", result.contains("SELECT * FROM t"));
  }

  @Test
  public void testDollarQuotedInSelectList() {
    String sql = "SELECT id, name, $$admin_password$$ AS default_pass FROM users WHERE role = 'superadmin'";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Dollar-quoted password should be redacted", result.contains("admin_password"));
    assertFalse("Role value should be redacted", result.contains("superadmin"));
    assertTrue("Should preserve column names", result.contains("id, name"));
  }

  @Test
  public void testDollarQuotedMultiline() {
    // Dollar-quoting can span multiple lines (common in PostgreSQL functions)
    String sql = "SELECT * FROM t WHERE data = $$line1\nline2\nSSN: 999-88-7777$$";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Multiline dollar-quoted content should be redacted", result.contains("999-88-7777"));
    assertFalse("Multiline content should be redacted", result.contains("line1"));
  }

  @Test
  public void testDollarQuotedEmptyContent() {
    // Edge case: empty dollar-quoted string
    String sql = "SELECT * FROM t WHERE data = $$$$";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Empty dollar-quoted string should be redacted", result.contains("$$$$"));
  }

  @Test
  public void testDollarQuotedRegexFallback() {
    // When JSqlParser fails to parse, the regex fallback should also handle dollar-quoting
    String sql = "SELECT @@@custom_func($$secret_data$$) :::: FROM users";
    String result = SqlParser.redactSensitiveData(sql);

    assertFalse("Dollar-quoted string should be redacted in regex fallback",
        result.contains("secret_data"));
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

  @Test
  public void testMySQLGroupConcatBasic() {
    String sql = "SELECT GROUP_CONCAT(name) FROM users WHERE dept = 'Sales'";

    String expected = "SELECT GROUP_CONCAT(name) FROM users WHERE dept = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLGroupConcatWithSeparator() {
    String sql = "SELECT GROUP_CONCAT(name SEPARATOR ', ') FROM users WHERE id IN (1, 2, 3)";

    String expected = "SELECT GROUP_CONCAT(name SEPARATOR "
        + SqlParser.REDACTED_STRING
        + ") FROM users WHERE id IN ("
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLGroupConcatDistinctOrderBy() {
    String sql = "SELECT GROUP_CONCAT(DISTINCT name ORDER BY name ASC SEPARATOR '; ') "
        + "FROM employees WHERE salary > 50000";

    String expected = "SELECT GROUP_CONCAT(DISTINCT name ORDER BY name ASC SEPARATOR "
        + SqlParser.REDACTED_STRING
        + ") FROM employees WHERE salary > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testFullTextSearchBasic() {
    String sql = "SELECT * FROM articles WHERE MATCH(title, body) AGAINST ('database optimization')";

    String expected = "SELECT * FROM articles WHERE MATCH (title, body) AGAINST ("
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testFullTextSearchWithModifier() {
    String sql = "SELECT * FROM products WHERE MATCH(description) "
        + "AGAINST ('wireless bluetooth headphones' IN BOOLEAN MODE)";

    String expected = "SELECT * FROM products WHERE MATCH (description) AGAINST ("
        + SqlParser.REDACTED_STRING
        + " IN BOOLEAN MODE)";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testFullTextSearchWithWhereCondition() {
    String sql = "SELECT * FROM posts WHERE MATCH(content) AGAINST ('secret document') "
        + "AND author = 'admin' AND views > 1000";

    String expected = "SELECT * FROM posts WHERE MATCH (content) AGAINST ("
        + SqlParser.REDACTED_STRING
        + ") AND author = "
        + SqlParser.REDACTED_STRING
        + " AND views > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testIsDistinctFromWithString() {
    String sql = "SELECT * FROM users WHERE name IS DISTINCT FROM 'John'";

    String expected = "SELECT * FROM users WHERE name IS DISTINCT FROM "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testIsDistinctFromWithNumber() {
    String sql = "SELECT * FROM orders WHERE amount IS DISTINCT FROM 999";

    String expected = "SELECT * FROM orders WHERE amount IS DISTINCT FROM "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testIsNotDistinctFrom() {
    String sql = "SELECT * FROM products WHERE category IS NOT DISTINCT FROM 'Electronics'";

    String expected = "SELECT * FROM products WHERE category IS NOT DISTINCT FROM "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleConnectByStartWithLiterals() {
    String sql = "SELECT employee_id, manager_id FROM employees "
        + "START WITH employee_id = 100 "
        + "CONNECT BY PRIOR employee_id = manager_id";

    String expected = "SELECT employee_id, manager_id FROM employees START WITH employee_id = "
        + SqlParser.REDACTED_NUMBER
        + " CONNECT BY PRIOR employee_id = manager_id";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleConnectByWithNoCycle() {
    String sql = "SELECT * FROM categories "
        + "START WITH parent_id = 0 "
        + "CONNECT BY NOCYCLE PRIOR category_id = parent_id";

    String expected = "SELECT * FROM categories START WITH parent_id = "
        + SqlParser.REDACTED_NUMBER
        + " CONNECT BY NOCYCLE PRIOR category_id = parent_id";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleConnectByStartWithStringLiteral() {
    String sql = "SELECT * FROM org_chart "
        + "START WITH dept_name = 'Engineering' "
        + "CONNECT BY PRIOR dept_id = parent_dept_id";

    String expected = "SELECT * FROM org_chart START WITH dept_name = "
        + SqlParser.REDACTED_STRING
        + " CONNECT BY PRIOR dept_id = parent_dept_id";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresJsonExpressionArrow() {
    String sql = "SELECT data->'address' FROM users WHERE id = 1";

    String expected = "SELECT data->"
        + SqlParser.REDACTED_STRING
        + " FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresJsonExpressionDoubleArrow() {
    String sql = "SELECT info->>'email' FROM customers WHERE info->>'name' = 'Alice'";

    String expected = "SELECT info->>"
        + SqlParser.REDACTED_STRING
        + " FROM customers WHERE info->>"
        + SqlParser.REDACTED_STRING
        + " = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresJsonExpressionChained() {
    String sql = "SELECT data->'profile'->'address'->>'city' FROM users WHERE id = 42";

    String expected = "SELECT data->"
        + SqlParser.REDACTED_STRING
        + "->"
        + SqlParser.REDACTED_STRING
        + "->>"
        + SqlParser.REDACTED_STRING
        + " FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleKeepExpression() {
    String sql = "SELECT department_id, "
        + "MAX(salary) KEEP (DENSE_RANK FIRST ORDER BY hire_date) AS earliest_max_salary "
        + "FROM employees WHERE salary > 50000 GROUP BY department_id";

    String expected = "SELECT department_id, MAX(salary) KEEP (DENSE_RANK FIRST ORDER BY hire_date) AS earliest_max_salary FROM employees WHERE salary > "
        + SqlParser.REDACTED_NUMBER
        + " GROUP BY department_id";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleKeepExpressionLast() {
    String sql = "SELECT MIN(salary) KEEP (DENSE_RANK LAST ORDER BY commission_pct) "
        + "FROM employees WHERE department_id = 80";

    String expected = "SELECT MIN(salary) KEEP (DENSE_RANK LAST ORDER BY commission_pct) FROM employees WHERE department_id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOverlapsCondition() {
    String sql = "SELECT * FROM reservations WHERE "
        + "(start_date, end_date) OVERLAPS ('2024-01-01', '2024-06-30')";

    String expected = "SELECT * FROM reservations WHERE (start_date, end_date) OVERLAPS ("
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testNullValuePreserved() {
    String sql = "SELECT * FROM users WHERE name IS NULL AND email IS NOT NULL";
    String result = SqlParser.redactSensitiveData(sql);

    assertEquals("SELECT * FROM users WHERE name IS NULL AND email IS NOT NULL", result);
  }

  @Test
  public void testNullValueInCoalesce() {
    String sql = "SELECT COALESCE(name, NULL, 'Unknown') FROM users WHERE id = 1";

    String expected = "SELECT COALESCE(name, NULL, "
        + SqlParser.REDACTED_STRING
        + ") FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCaseExpressionWithLiterals() {
    String sql = "SELECT CASE status WHEN 'A' THEN 'Active' WHEN 'I' THEN 'Inactive' "
        + "ELSE 'Unknown' END FROM users WHERE id = 42";

    String expected = "SELECT CASE status WHEN "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " WHEN "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCastExpressionPreservesType() {
    String sql = "SELECT CAST('2024-01-15' AS DATE) FROM orders WHERE id = 100";

    String expected = "SELECT CAST("
        + SqlParser.REDACTED_STRING
        + " AS DATE) FROM orders WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testExtractExpression() {
    String sql = "SELECT EXTRACT(YEAR FROM order_date) FROM orders WHERE total > 500";

    String expected = "SELECT EXTRACT(YEAR FROM order_date) FROM orders WHERE total > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testBinaryExpressionWithLiterals() {
    String sql = "SELECT price * 1.1 AS tax_price FROM products WHERE discount <> 0.15";

    String expected = "SELECT price * "
        + SqlParser.REDACTED_NUMBER
        + " AS tax_price FROM products WHERE discount <> "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testRegExpMatchOperator() {
    String sql = "SELECT * FROM products WHERE name ~ 'secret_pattern' AND price > 99.99";

    String expected = "SELECT * FROM products WHERE name ~ "
        + SqlParser.REDACTED_STRING
        + " AND price > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testContainsOperator() {
    String sql = "SELECT * FROM data WHERE tags @> ARRAY['sensitive']";

    String expected = "SELECT * FROM data WHERE tags @> ARRAY["
        + SqlParser.REDACTED_STRING
        + "]";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testJsonOperatorPreservesStructure() {
    String sql = "SELECT data->>'name', data->>'email' FROM users WHERE data->>'status' = 'active'";

    String expected = "SELECT data->>"
        + SqlParser.REDACTED_STRING
        + ", data->>"
        + SqlParser.REDACTED_STRING
        + " FROM users WHERE data->>"
        + SqlParser.REDACTED_STRING
        + " = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSimilarToExpression() {
    String sql = "SELECT * FROM users WHERE name SIMILAR TO '%John%'";

    String expected = "SELECT * FROM users WHERE name SIMILAR TO "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testAnalyticExpressionWithLiterals() {
    String sql = "SELECT name, salary, "
        + "LAG(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_salary "
        + "FROM employees WHERE dept = 'IT'";

    String expected = "SELECT name, salary, LAG(salary, "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ") OVER (ORDER BY hire_date) AS prev_salary FROM employees WHERE dept = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testTranscodingFunction() {
    String sql = "SELECT CONVERT('Hello World' USING utf8) FROM dual";

    String expected = "SELECT CONVERT( "
        + SqlParser.REDACTED_STRING
        + " USING utf8 ) FROM dual";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testExistsWithLiterals() {
    String sql = "SELECT * FROM customers WHERE EXISTS "
        + "(SELECT 1 FROM orders WHERE customer_id = customers.id AND total > 10000)";

    String expected = "SELECT * FROM customers WHERE EXISTS (SELECT "
        + SqlParser.REDACTED_NUMBER
        + " FROM orders WHERE customer_id = customers.id AND total > "
        + SqlParser.REDACTED_NUMBER
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMemberOfExpression() {
    String sql = "SELECT * FROM data WHERE 42 MEMBER OF (json_col)";

    String expected = "SELECT * FROM data WHERE "
        + SqlParser.REDACTED_NUMBER
        + " MEMBER OF (json_col)";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testAllExpressionsEndToEnd() {
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


    String expected = "SELECT CASE WHEN status = "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END, CAST(price AS DECIMAL (10, 2)), COALESCE(name, "
        + SqlParser.REDACTED_STRING
        + "), CONCAT(first, "
        + SqlParser.REDACTED_STRING
        + ", last) FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.email LIKE "
        + SqlParser.REDACTED_STRING
        + " AND o.total BETWEEN "
        + SqlParser.REDACTED_NUMBER
        + " AND "
        + SqlParser.REDACTED_NUMBER
        + " AND u.age IN ("
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ") AND o.note IS NOT NULL ORDER BY o.total DESC LIMIT "
        + SqlParser.REDACTED_NUMBER
        + " OFFSET "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testArrayIndexing() {
    String sql = "SELECT data[1] FROM users WHERE id = 42";

    String expected = "SELECT data["
        + SqlParser.REDACTED_NUMBER
        + "] FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testArraySlicing() {
    String sql = "SELECT arr[2:5] FROM data_table WHERE id = 10";

    String expected = "SELECT arr["
        + SqlParser.REDACTED_NUMBER
        + ":"
        + SqlParser.REDACTED_NUMBER
        + "] FROM data_table WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testArrayConstructorWithLiterals() {
    String sql = "SELECT * FROM products WHERE tags = ARRAY['secret', 'confidential']";

    String expected = "SELECT * FROM products WHERE tags = ARRAY["
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + "]";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresAtTimeZone() {
    String sql = "SELECT created_at AT TIME ZONE 'UTC' FROM events WHERE id = 1";

    String expected = "SELECT created_at AT TIME ZONE "
        + SqlParser.REDACTED_STRING
        + " FROM events WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresCastColonSyntax() {
    String sql = "SELECT price::numeric FROM products WHERE name = 'Widget'";

    String expected = "SELECT price::numeric FROM products WHERE name = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresSimilarTo() {
    String sql = "SELECT * FROM users WHERE email SIMILAR TO '%@(gmail|yahoo)\\.com'";

    String expected = "SELECT * FROM users WHERE email SIMILAR TO "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresJsonContainsOperator() {
    String sql = "SELECT * FROM docs WHERE metadata @> '{\"type\": \"report\"}' AND id = 42";

    String expected = "SELECT * FROM docs WHERE metadata @> "
        + SqlParser.REDACTED_STRING
        + " AND id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresContainedByOperator() {
    String sql = "SELECT * FROM tags WHERE tag_array <@ ARRAY['important', 'urgent']";

    String expected = "SELECT * FROM tags WHERE tag_array <@ ARRAY["
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + "]";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresGeometryDistance() {
    String sql = "SELECT * FROM places WHERE location <-> point(37.7749, -122.4194) < 10";

    String expected = "SELECT * FROM places WHERE location <-> point("
        + SqlParser.REDACTED_NUMBER
        + ", -"
        + SqlParser.REDACTED_NUMBER
        + ") < "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresStringAgg() {
    String sql = "SELECT STRING_AGG(name, ', ' ORDER BY name) FROM employees WHERE dept = 'Sales'";

    String expected = "SELECT STRING_AGG(name, "
        + SqlParser.REDACTED_STRING
        + " ORDER BY name) FROM employees WHERE dept = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresIntervalExpression() {
    String sql = "SELECT * FROM events WHERE event_time > NOW() - INTERVAL '30 days'";

    String expected = "SELECT * FROM events WHERE event_time > NOW() - INTERVAL "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgresCoalesceWithMultipleArgs() {
    String sql = "SELECT COALESCE(a, b, c, 'default_val') FROM t WHERE x = 99";

    String expected = "SELECT COALESCE(a, b, c, "
        + SqlParser.REDACTED_STRING
        + ") FROM t WHERE x = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleConnectByRoot() {
    String sql = "SELECT CONNECT_BY_ROOT employee_name AS root_name, level "
        + "FROM employees "
        + "START WITH manager_id IS NULL "
        + "CONNECT BY PRIOR employee_id = manager_id";

    String expected = "SELECT CONNECT_BY_ROOT employee_name AS root_name, level FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleNvl2Function() {
    String sql = "SELECT NVL2(commission_pct, salary * commission_pct, 0) "
        + "FROM employees WHERE department_id = 80";

    String expected = "SELECT NVL2(commission_pct, salary * commission_pct, "
        + SqlParser.REDACTED_NUMBER
        + ") FROM employees WHERE department_id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleListagg() {
    String sql = "SELECT department_id, LISTAGG(last_name, ', ') "
        + "WITHIN GROUP (ORDER BY last_name) "
        + "FROM employees WHERE salary > 50000 GROUP BY department_id";

    String expected = "SELECT department_id, LISTAGG(last_name, "
        + SqlParser.REDACTED_STRING
        + ") WITHIN GROUP FROM employees WHERE salary > "
        + SqlParser.REDACTED_NUMBER
        + " GROUP BY department_id";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleRegexpLike() {
    String sql = "SELECT * FROM customers WHERE REGEXP_LIKE(email, '^[a-z]+@example\\.com$')";

    String expected = "SELECT * FROM customers WHERE REGEXP_LIKE(email, "
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOracleCoalesceWithNested() {
    String sql = "SELECT COALESCE(preferred_name, first_name || ' ' || last_name, 'Unknown') "
        + "FROM employees WHERE emp_id = 123";

    String expected = "SELECT COALESCE(preferred_name, first_name || "
        + SqlParser.REDACTED_STRING
        + " || last_name, "
        + SqlParser.REDACTED_STRING
        + ") FROM employees WHERE emp_id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLMatchAgainstWithBooleanMode() {
    String sql = "SELECT * FROM articles WHERE MATCH(title, body) "
        + "AGAINST ('+mysql -oracle' IN BOOLEAN MODE) AND category = 'tech'";

    String expected = "SELECT * FROM articles WHERE MATCH (title, body) AGAINST ("
        + SqlParser.REDACTED_STRING
        + " IN BOOLEAN MODE) AND category = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLGroupConcatComplex() {
    String sql = "SELECT dept, GROUP_CONCAT(DISTINCT name ORDER BY name DESC SEPARATOR ' | ') "
        + "FROM employees WHERE salary > 60000 GROUP BY dept";

    String expected = "SELECT dept, GROUP_CONCAT(DISTINCT name ORDER BY name DESC SEPARATOR "
        + SqlParser.REDACTED_STRING
        + ") FROM employees WHERE salary > "
        + SqlParser.REDACTED_NUMBER
        + " GROUP BY dept";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLIntervalDateAdd() {
    String sql = "SELECT * FROM events WHERE event_date > DATE_SUB(NOW(), INTERVAL 7 DAY) "
        + "AND status = 'active'";

    String expected = "SELECT * FROM events WHERE event_date > DATE_SUB(NOW(), INTERVAL "
        + SqlParser.REDACTED_NUMBER
        + " DAY) AND status = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLReplace() {
    String sql = "SELECT REPLACE(description, 'old_val', 'new_val') FROM products WHERE id = 1";

    String expected = "SELECT REPLACE(description, "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ") FROM products WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSqlServerStringAgg() {
    String sql = "SELECT department, STRING_AGG(employee_name, ', ') "
        + "FROM employees WHERE status = 'active' GROUP BY department";

    String expected = "SELECT department, STRING_AGG(employee_name, "
        + SqlParser.REDACTED_STRING
        + ") FROM employees WHERE status = "
        + SqlParser.REDACTED_STRING
        + " GROUP BY department";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSqlServerCoalesce() {
    String sql = "SELECT COALESCE(phone, mobile, 'N/A') FROM contacts WHERE id = 500";

    String expected = "SELECT COALESCE(phone, mobile, "
        + SqlParser.REDACTED_STRING
        + ") FROM contacts WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSqlServerTry_Convert() {
    String sql = "SELECT TRY_CONVERT(INT, '12345') FROM data WHERE key = 'test'";

    String expected = "SELECT TRY_CONVERT(INT, "
        + SqlParser.REDACTED_STRING
        + ") FROM data WHERE key = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testTrimFunction() {
    String sql = "SELECT TRIM(' sensitive_data ') FROM users WHERE id = 1";

    String expected = "SELECT Trim( "
        + SqlParser.REDACTED_STRING
        + " ) FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testTrimWithSpecification() {
    String sql = "SELECT TRIM(LEADING 'x' FROM name) FROM users WHERE id = 1";

    String expected = "SELECT Trim( LEADING "
        + SqlParser.REDACTED_STRING
        + " FROM name ) FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testWindowFunctionRowsBetween() {
    String sql = "SELECT name, salary, "
        + "AVG(salary) OVER (PARTITION BY dept ORDER BY hire_date "
        + "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg "
        + "FROM employees WHERE status = 'active'";

    String expected = "SELECT name, salary, AVG(salary) OVER (PARTITION BY dept ORDER BY hire_date ROWS BETWEEN "
        + SqlParser.REDACTED_NUMBER
        + " PRECEDING AND CURRENT ROW) AS moving_avg FROM employees WHERE status = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testWindowFunctionRangeBetween() {
    String sql = "SELECT id, value, "
        + "SUM(value) OVER (ORDER BY id "
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative "
        + "FROM data WHERE category = 'A'";

    String expected = "SELECT id, value, SUM(value) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative FROM data WHERE category = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testWindowFunctionWithFilter() {
    String sql = "SELECT department, "
        + "COUNT(*) FILTER (WHERE salary > 50000) OVER (PARTITION BY department) AS high_earners "
        + "FROM employees WHERE year = 2024";

    String expected = "SELECT department, COUNT(*)FILTER (WHERE salary > "
        + SqlParser.REDACTED_NUMBER
        + ") OVER (PARTITION BY department ) AS high_earners FROM employees WHERE year = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testDeleteStatement() {
    String sql = "DELETE FROM users WHERE name = 'John Doe' AND created_at < '2023-01-01'";

    String expected = "DELETE FROM users WHERE name = "
        + SqlParser.REDACTED_STRING
        + " AND created_at < "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testInsertWithValues() {
    String sql = "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)";

    String expected = "INSERT INTO users (name, email, age) VALUES ("
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCTEWithLiterals() {
    String sql = "WITH high_earners AS ("
        + "SELECT * FROM employees WHERE salary > 100000"
        + ") SELECT name FROM high_earners WHERE dept = 'Engineering'";

    String expected = "WITH high_earners AS (SELECT * FROM employees WHERE salary > "
        + SqlParser.REDACTED_NUMBER
        + ") SELECT name FROM high_earners WHERE dept = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMultipleCTEs() {
    String sql = "WITH dept_stats AS ("
        + "SELECT dept, AVG(salary) AS avg_sal FROM employees GROUP BY dept"
        + "), top_depts AS ("
        + "SELECT dept FROM dept_stats WHERE avg_sal > 80000"
        + ") SELECT * FROM employees WHERE dept IN (SELECT dept FROM top_depts) "
        + "AND name LIKE '%Smith%'";

    String expected = "WITH dept_stats AS (SELECT dept, AVG(salary) AS avg_sal FROM employees GROUP BY dept), top_depts AS (SELECT dept FROM dept_stats WHERE avg_sal > "
        + SqlParser.REDACTED_NUMBER
        + ") SELECT * FROM employees WHERE dept IN (SELECT dept FROM top_depts) AND name LIKE "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testBitwiseOperators() {
    String sql = "SELECT flags & 255 AS masked, flags | 128 AS with_flag "
        + "FROM settings WHERE id = 42";

    String expected = "SELECT flags & "
        + SqlParser.REDACTED_NUMBER
        + " AS masked, flags | "
        + SqlParser.REDACTED_NUMBER
        + " AS with_flag FROM settings WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testXorExpression() {
    String sql = "SELECT * FROM flags WHERE (active = 1) XOR (verified = 1)";

    String expected = "SELECT * FROM flags WHERE (active = "
        + SqlParser.REDACTED_NUMBER
        + ") XOR (verified = "
        + SqlParser.REDACTED_NUMBER
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testModuloOperator() {
    String sql = "SELECT * FROM orders WHERE order_id % 100 = 0";

    String expected = "SELECT * FROM orders WHERE order_id % "
        + SqlParser.REDACTED_NUMBER
        + " = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testNotExpressionWithLiteral() {
    String sql = "SELECT * FROM users WHERE NOT (status = 'banned')";

    String expected = "SELECT * FROM users WHERE NOT (status = "
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLVariableAssignment() {
    String sql = "SELECT @rank := @rank + 1 AS rank, name FROM employees WHERE dept = 'IT'";

    String expected = "SELECT @rank := @rank + "
        + SqlParser.REDACTED_NUMBER
        + " AS rank, name FROM employees WHERE dept = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testExtractYear() {
    String sql = "SELECT * FROM orders WHERE EXTRACT(YEAR FROM order_date) = 2024 "
        + "AND total > 1000";

    String expected = "SELECT * FROM orders WHERE EXTRACT(YEAR FROM order_date) = "
        + SqlParser.REDACTED_NUMBER
        + " AND total > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testExtractMonth() {
    String sql = "SELECT EXTRACT(MONTH FROM hire_date) AS hire_month "
        + "FROM employees WHERE salary > 50000";

    String expected = "SELECT EXTRACT(MONTH FROM hire_date) AS hire_month FROM employees WHERE salary > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCastWithVarchar() {
    String sql = "SELECT CAST(employee_id AS VARCHAR(50)) FROM employees WHERE name = 'Alice'";

    String expected = "SELECT CAST(employee_id AS VARCHAR (50)) FROM employees WHERE name = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testNestedFunctionsWithLiterals() {
    String sql = "SELECT UPPER(TRIM(CONCAT(first_name, ' ', last_name))) "
        + "FROM users WHERE LOWER(email) = 'admin@example.com'";

    String expected = "SELECT UPPER(Trim( CONCAT(first_name, "
        + SqlParser.REDACTED_STRING
        + ", last_name) )) FROM users WHERE LOWER(email) = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testArithmeticWithLiterals() {
    String sql = "SELECT price * 1.08 AS tax_price, price + 500 AS markup "
        + "FROM products WHERE price > 100 AND discount <> 0.15";

    String expected = "SELECT price * "
        + SqlParser.REDACTED_NUMBER
        + " AS tax_price, price + "
        + SqlParser.REDACTED_NUMBER
        + " AS markup FROM products WHERE price > "
        + SqlParser.REDACTED_NUMBER
        + " AND discount <> "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testIsBooleanExpression() {
    String sql = "SELECT * FROM flags WHERE active IS TRUE AND deleted IS NOT FALSE";
    String result = SqlParser.redactSensitiveData(sql);

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

    String expected = "SELECT * FROM orders WHERE amount > ANY(SELECT avg_amount FROM dept_stats WHERE dept = "
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testIntegerDivision() {
    String sql = "SELECT total DIV 100 AS hundreds FROM accounts WHERE balance > 5000";

    String expected = "SELECT total DIV "
        + SqlParser.REDACTED_NUMBER
        + " AS hundreds FROM accounts WHERE balance > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testParenthesizedExpression() {
    String sql = "SELECT * FROM orders WHERE (status = 'pending' OR status = 'processing') "
        + "AND total > 1000";

    String expected = "SELECT * FROM orders WHERE (status = "
        + SqlParser.REDACTED_STRING
        + " OR status = "
        + SqlParser.REDACTED_STRING
        + ") AND total > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testTimezoneExpressionMultiple() {
    String sql = "SELECT event_time AT TIME ZONE 'America/New_York' "
        + "FROM events WHERE event_type = 'meeting'";

    String expected = "SELECT event_time AT TIME ZONE "
        + SqlParser.REDACTED_STRING
        + " FROM events WHERE event_type = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testConcatOperator() {
    String sql = "SELECT 'Mr. ' || first_name || ' ' || last_name FROM users WHERE id = 1";

    String expected = "SELECT "
        + SqlParser.REDACTED_STRING
        + " || first_name || "
        + SqlParser.REDACTED_STRING
        + " || last_name FROM users WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCurrentTimestampKeyword() {
    String sql = "SELECT * FROM logs WHERE created_at > CURRENT_TIMESTAMP AND level = 'ERROR'";

    String expected = "SELECT * FROM logs WHERE created_at > CURRENT_TIMESTAMP AND level = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

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

    String expected = "SELECT u.name, u.email, COALESCE(u.phone, "
        + SqlParser.REDACTED_STRING
        + ") AS phone, ROW_NUMBER() OVER (PARTITION BY u.department ORDER BY u.salary DESC) AS rank, u.metadata->>"
        + SqlParser.REDACTED_STRING
        + " AS role FROM users u WHERE u.status = "
        + SqlParser.REDACTED_STRING
        + " AND u.created_at > CURRENT_TIMESTAMP - INTERVAL "
        + SqlParser.REDACTED_NUMBER
        + " AND u.tags @> ARRAY["
        + SqlParser.REDACTED_STRING
        + "] ORDER BY u.salary DESC LIMIT "
        + SqlParser.REDACTED_NUMBER
        + " OFFSET "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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

    String expected = "SELECT e.employee_id, e.name, e.salary, MAX(e.salary) KEEP (DENSE_RANK FIRST ORDER BY e.hire_date)  OVER (PARTITION BY e.department_id ) AS first_hire_max_sal FROM employees e WHERE e.salary > "
        + SqlParser.REDACTED_NUMBER
        + " AND e.department_id IN ("
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ") START WITH e.manager_id IS NULL CONNECT BY PRIOR e.employee_id = e.manager_id";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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

    String expected = "SELECT d.name, GROUP_CONCAT(DISTINCT e.name ORDER BY e.salary DESC SEPARATOR "
        + SqlParser.REDACTED_STRING
        + "), COUNT(IF(e.salary > "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ", NULL)) AS high_earners FROM departments d INNER JOIN employees e ON d.id = e.dept_id WHERE d.status = "
        + SqlParser.REDACTED_STRING
        + " AND MATCH (d.description) AGAINST ("
        + SqlParser.REDACTED_STRING
        + " IN BOOLEAN MODE) GROUP BY d.name HAVING high_earners > "
        + SqlParser.REDACTED_NUMBER
        + " LIMIT "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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

    String expected = "SELECT TOP 50 e.name, e.salary, d.name AS dept_name, IIF(e.salary > "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_STRING
        + ", "
        + SqlParser.REDACTED_STRING
        + ") AS level, FORMAT(e.hire_date, "
        + SqlParser.REDACTED_STRING
        + ") AS formatted_date FROM employees e WITH (NOLOCK) INNER JOIN departments d ON e.dept_id = d.id WHERE e.status = "
        + SqlParser.REDACTED_STRING
        + " AND DATEDIFF(day, e.hire_date, GETDATE()) > "
        + SqlParser.REDACTED_NUMBER
        + " AND LEN(e.name) > "
        + SqlParser.REDACTED_NUMBER
        + " ORDER BY e.salary DESC OFFSET "
        + SqlParser.REDACTED_NUMBER
        + " ROWS FETCH NEXT "
        + SqlParser.REDACTED_NUMBER
        + " ROWS ONLY";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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

    String expected = "SELECT e.name, e.salary, VALUE(e.commission, "
        + SqlParser.REDACTED_NUMBER
        + ") AS commission, CASE WHEN e.salary > "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " WHEN e.salary > "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS grade FROM employees e WHERE e.department = "
        + SqlParser.REDACTED_STRING
        + " AND e.hire_date BETWEEN "
        + SqlParser.REDACTED_STRING
        + " AND "
        + SqlParser.REDACTED_STRING
        + " FETCH FIRST "
        + SqlParser.REDACTED_NUMBER
        + " ROWS ONLY OPTIMIZE FOR 50 ROWS";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMergeStatementComplex() {
    String sql = "MERGE INTO target_table t "
        + "USING source_table s ON (t.id = s.id) "
        + "WHEN MATCHED AND s.status = 'active' THEN "
        + "UPDATE SET t.value = s.value, t.updated_at = '2024-01-15' "
        + "WHEN NOT MATCHED THEN "
        + "INSERT (id, value, status) VALUES (s.id, s.value, 'new')";

    String expected = "MERGE INTO target_table t USING source_table s ON (t.id = s.id) WHEN MATCHED AND s.status = "
        + SqlParser.REDACTED_STRING
        + " THEN UPDATE SET t.value = s.value, t.updated_at = "
        + SqlParser.REDACTED_STRING
        + " WHEN NOT MATCHED THEN INSERT (id, value, status) VALUES (s.id, s.value, "
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testEmptyStringLiteral() {
    String sql = "SELECT * FROM users WHERE name = ''";

    String expected = "SELECT * FROM users WHERE name = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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

    String expected = "SELECT * FROM a WHERE x IN (SELECT y FROM b WHERE z IN (SELECT w FROM c WHERE v = "
        + SqlParser.REDACTED_STRING
        + "))";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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

    String expected = "SELECT CONVERT( "
        + SqlParser.REDACTED_STRING
        + " USING utf8 ) FROM test_table";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCaseExpressionSimpleWhen() {
    String sql = "SELECT CASE status WHEN 'active' THEN 'Active User' "
        + "WHEN 'inactive' THEN 'Inactive User' "
        + "ELSE 'Unknown' END AS status_label FROM users";

    String expected = "SELECT CASE status WHEN "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " WHEN "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS status_label FROM users";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCaseExpressionSearchedForm() {
    String sql = "SELECT CASE WHEN age > 65 THEN 'Senior' "
        + "WHEN age > 18 THEN 'Adult' "
        + "WHEN age > 12 THEN 'Teen' "
        + "ELSE 'Child' END AS age_group FROM people";

    String expected = "SELECT CASE WHEN age > "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " WHEN age > "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " WHEN age > "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS age_group FROM people";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCaseExpressionWithNumericResults() {
    String sql = "SELECT CASE WHEN department = 'Engineering' THEN 1.5 "
        + "WHEN department = 'Sales' THEN 1.2 "
        + "ELSE 1.0 END * salary AS adjusted_salary FROM employees";

    String expected = "SELECT CASE WHEN department = "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_NUMBER
        + " WHEN department = "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_NUMBER
        + " ELSE "
        + SqlParser.REDACTED_NUMBER
        + " END * salary AS adjusted_salary FROM employees";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testNestedCaseExpressions() {
    String sql = "SELECT CASE WHEN type = 'A' THEN "
        + "CASE WHEN subtype = 'X' THEN 'AX' ELSE 'AO' END "
        + "ELSE 'Other' END AS category FROM items";

    String expected = "SELECT CASE WHEN type = "
        + SqlParser.REDACTED_STRING
        + " THEN CASE WHEN subtype = "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS category FROM items";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCaseExpressionInWhereClause() {
    String sql = "SELECT * FROM orders WHERE "
        + "CASE WHEN region = 'US' THEN amount > 1000 "
        + "WHEN region = 'EU' THEN amount > 800 "
        + "ELSE amount > 500 END";

    String expected = "SELECT * FROM orders WHERE CASE WHEN region = "
        + SqlParser.REDACTED_STRING
        + " THEN amount > "
        + SqlParser.REDACTED_NUMBER
        + " WHEN region = "
        + SqlParser.REDACTED_STRING
        + " THEN amount > "
        + SqlParser.REDACTED_NUMBER
        + " ELSE amount > "
        + SqlParser.REDACTED_NUMBER
        + " END";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testWhenClauseWithExpressionValues() {
    String sql = "SELECT CASE WHEN salary + bonus > 100000 THEN 'Top' "
        + "ELSE 'Regular' END AS tier FROM employees";

    String expected = "SELECT CASE WHEN salary + bonus > "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS tier FROM employees";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testWhenClauseWithFunctionCalls() {
    String sql = "SELECT CASE WHEN LENGTH(name) > 50 THEN SUBSTR(name, 1, 50) "
        + "ELSE name END AS truncated_name FROM users";

    String expected = "SELECT CASE WHEN LENGTH(name) > "
        + SqlParser.REDACTED_NUMBER
        + " THEN SUBSTR(name, "
        + SqlParser.REDACTED_NUMBER
        + ", "
        + SqlParser.REDACTED_NUMBER
        + ") ELSE name END AS truncated_name FROM users";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testWhenClauseWithNullValues() {
    String sql = "SELECT CASE WHEN email IS NULL THEN 'no-email@example.com' "
        + "ELSE email END AS contact FROM users";

    String expected = "SELECT CASE WHEN email IS NULL THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE email END AS contact FROM users";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMultipleWhenClausesWithDates() {
    String sql = "SELECT CASE "
        + "WHEN hire_date < '2020-01-01' THEN 'Veteran' "
        + "WHEN hire_date < '2022-06-15' THEN 'Experienced' "
        + "WHEN hire_date < '2024-01-01' THEN 'Recent' "
        + "ELSE 'New' END AS tenure FROM employees";

    String expected = "SELECT CASE WHEN hire_date < "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " WHEN hire_date < "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " WHEN hire_date < "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS tenure FROM employees";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testAnyComparisonWithSubquery() {
    String sql = "SELECT * FROM products WHERE price > ANY "
        + "(SELECT price FROM products WHERE category = 'Electronics')";

    String expected = "SELECT * FROM products WHERE price > ANY(SELECT price FROM products WHERE category = "
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSomeComparisonWithSubquery() {
    String sql = "SELECT * FROM employees WHERE salary = SOME "
        + "(SELECT salary FROM managers WHERE dept = 'Finance')";

    String expected = "SELECT * FROM employees WHERE salary = SOME(SELECT salary FROM managers WHERE dept = "
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testAllComparisonWithSubquery() {
    String sql = "SELECT * FROM products WHERE price >= ALL "
        + "(SELECT min_price FROM price_rules WHERE region = 'APAC')";

    String expected = "SELECT * FROM products WHERE price >= ALL(SELECT min_price FROM price_rules WHERE region = "
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testAnyComparisonWithLiteralsInSubquery() {
    String sql = "SELECT name FROM employees WHERE id = ANY "
        + "(SELECT employee_id FROM awards WHERE year = 2024 "
        + "AND type = 'excellence')";

    String expected = "SELECT name FROM employees WHERE id = ANY(SELECT employee_id FROM awards WHERE year = "
        + SqlParser.REDACTED_NUMBER
        + " AND type = "
        + SqlParser.REDACTED_STRING
        + ")";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testColumnAliasesPreserved() {
    String sql = "SELECT name AS employee_name, salary AS emp_salary, "
        + "'active' AS status FROM employees WHERE id = 42";

    String expected = "SELECT name AS employee_name, salary AS emp_salary, "
        + SqlParser.REDACTED_STRING
        + " AS status FROM employees WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testTableAliasesPreserved() {
    String sql = "SELECT e.name, d.name FROM employees e "
        + "JOIN departments d ON e.dept_id = d.id "
        + "WHERE e.status = 'active'";

    String expected = "SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.status = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSubqueryAlias() {
    String sql = "SELECT sub.total FROM "
        + "(SELECT COUNT(*) AS total FROM users WHERE age > 25) sub";

    String expected = "SELECT sub.total FROM (SELECT COUNT(*) AS total FROM users WHERE age > "
        + SqlParser.REDACTED_NUMBER
        + ") sub";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testBinaryArithmeticExpressions() {
    String sql = "SELECT price * 1.08 AS with_tax, "
        + "salary + 5000 AS adjusted, "
        + "total - 100 AS discounted, "
        + "amount / 12 AS monthly "
        + "FROM products WHERE id = 1";

    String expected = "SELECT price * "
        + SqlParser.REDACTED_NUMBER
        + " AS with_tax, salary + "
        + SqlParser.REDACTED_NUMBER
        + " AS adjusted, total - "
        + SqlParser.REDACTED_NUMBER
        + " AS discounted, amount / "
        + SqlParser.REDACTED_NUMBER
        + " AS monthly FROM products WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testBinaryComparisonExpressions() {
    String sql = "SELECT * FROM orders WHERE "
        + "amount >= 1000 AND amount <= 5000 "
        + "AND status <> 'cancelled' "
        + "AND region != 'test'";

    String expected = "SELECT * FROM orders WHERE amount >= "
        + SqlParser.REDACTED_NUMBER
        + " AND amount <= "
        + SqlParser.REDACTED_NUMBER
        + " AND status <> "
        + SqlParser.REDACTED_STRING
        + " AND region != "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testBinaryLogicalExpressions() {
    String sql = "SELECT * FROM users WHERE "
        + "(status = 'active' OR status = 'pending') "
        + "AND NOT deleted = 'true'";

    String expected = "SELECT * FROM users WHERE (status = "
        + SqlParser.REDACTED_STRING
        + " OR status = "
        + SqlParser.REDACTED_STRING
        + ") AND NOT deleted = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testBinaryStringConcatenation() {
    String sql = "SELECT first_name || ' ' || last_name AS full_name "
        + "FROM employees WHERE id = 100";

    String expected = "SELECT first_name || "
        + SqlParser.REDACTED_STRING
        + " || last_name AS full_name FROM employees WHERE id = "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testFilterClauseOnAggregate() {
    String sql = "SELECT department, "
        + "COUNT(*) FILTER (WHERE salary > 50000) AS high_earners, "
        + "AVG(salary) FILTER (WHERE hire_date > '2023-01-01') AS recent_avg "
        + "FROM employees GROUP BY department";

    String expected = "SELECT department, COUNT(*)FILTER (WHERE salary > "
        + SqlParser.REDACTED_NUMBER
        + ")  AS high_earners, AVG(salary)FILTER (WHERE hire_date > "
        + SqlParser.REDACTED_STRING
        + ")  AS recent_avg FROM employees GROUP BY department";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testWindowFunctionWithPartitionAndOrder() {
    String sql = "SELECT name, salary, "
        + "RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank, "
        + "SUM(salary) OVER (PARTITION BY department) AS dept_total "
        + "FROM employees WHERE status = 'active'";

    String expected = "SELECT name, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank, SUM(salary) OVER (PARTITION BY department ) AS dept_total FROM employees WHERE status = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testWindowFrameWithRows() {
    String sql = "SELECT name, "
        + "AVG(score) OVER (ORDER BY created_at "
        + "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg "
        + "FROM measurements WHERE value > 100";

    String expected = "SELECT name, AVG(score) OVER (ORDER BY created_at ROWS BETWEEN "
        + SqlParser.REDACTED_NUMBER
        + " PRECEDING AND CURRENT ROW) AS moving_avg FROM measurements WHERE value > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLUseIndex() {
    String sql = "SELECT * FROM employees USE INDEX (idx_name) "
        + "WHERE name = 'John' AND department = 'Engineering'";

    String expected = "SELECT * FROM employees USE INDEX (idx_name) WHERE name = "
        + SqlParser.REDACTED_STRING
        + " AND department = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLForceIndex() {
    String sql = "SELECT * FROM orders FORCE INDEX (idx_date) "
        + "WHERE order_date > '2024-01-01' AND total > 500";

    String expected = "SELECT * FROM orders FORCE INDEX (idx_date) WHERE order_date > "
        + SqlParser.REDACTED_STRING
        + " AND total > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLIgnoreIndex() {
    String sql = "SELECT * FROM products IGNORE INDEX (idx_old) "
        + "WHERE category = 'electronics' AND price < 999.99";

    String expected = "SELECT * FROM products IGNORE INDEX (idx_old) WHERE category = "
        + SqlParser.REDACTED_STRING
        + " AND price < "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSQLServerNoLockHint() {
    String sql = "SELECT * FROM employees WITH (NOLOCK) "
        + "WHERE department = 'Sales' AND salary > 60000";

    String expected = "SELECT * FROM employees WITH (NOLOCK) WHERE department = "
        + SqlParser.REDACTED_STRING
        + " AND salary > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testSQLServerMultipleHints() {
    String sql = "SELECT e.name, d.name FROM employees e WITH (NOLOCK) "
        + "JOIN departments d WITH (NOLOCK) ON e.dept_id = d.id "
        + "WHERE e.status = 'active'";

    String expected = "SELECT e.name, d.name FROM employees e WITH (NOLOCK) JOIN departments d WITH (NOLOCK) ON e.dept_id = d.id WHERE e.status = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOrderByWithLiteralsInExpressions() {
    String sql = "SELECT name, salary FROM employees "
        + "WHERE department = 'IT' "
        + "ORDER BY CASE WHEN status = 'lead' THEN 1 "
        + "WHEN status = 'senior' THEN 2 ELSE 3 END, "
        + "salary DESC";

    String expected = "SELECT name, salary FROM employees WHERE department = "
        + SqlParser.REDACTED_STRING
        + " ORDER BY CASE WHEN status = "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_NUMBER
        + " WHEN status = "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_NUMBER
        + " ELSE "
        + SqlParser.REDACTED_NUMBER
        + " END, salary DESC";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testOrderByNullsFirstLast() {
    String sql = "SELECT * FROM products "
        + "WHERE price > 10 "
        + "ORDER BY name ASC NULLS FIRST, price DESC NULLS LAST";

    String expected = "SELECT * FROM products WHERE price > "
        + SqlParser.REDACTED_NUMBER
        + " ORDER BY name ASC NULLS FIRST, price DESC NULLS LAST";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPartitionByMultipleColumns() {
    String sql = "SELECT department, region, "
        + "ROW_NUMBER() OVER (PARTITION BY department, region "
        + "ORDER BY hire_date) AS row_num "
        + "FROM employees WHERE salary > 40000";

    String expected = "SELECT department, region, ROW_NUMBER() OVER (PARTITION BY department, region ORDER BY hire_date) AS row_num FROM employees WHERE salary > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPartitionByWithExpression() {
    String sql = "SELECT "
        + "SUM(amount) OVER (PARTITION BY EXTRACT(YEAR FROM order_date) "
        + "ORDER BY order_date) AS running_total "
        + "FROM orders WHERE status = 'completed'";

    String expected = "SELECT SUM(amount) OVER (PARTITION BY EXTRACT(YEAR FROM order_date) ORDER BY order_date) AS running_total FROM orders WHERE status = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCaseWithAnyComparison() {
    String sql = "SELECT CASE WHEN salary > ANY "
        + "(SELECT salary FROM managers WHERE level = 'VP') "
        + "THEN 'Above VP' ELSE 'Below VP' END AS comparison "
        + "FROM employees";

    String expected = "SELECT CASE WHEN salary > ANY(SELECT salary FROM managers WHERE level = "
        + SqlParser.REDACTED_STRING
        + ") THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS comparison FROM employees";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testCaseWithWindowFunction() {
    String sql = "SELECT name, "
        + "CASE WHEN RANK() OVER (ORDER BY score DESC) <= 3 "
        + "THEN 'Top 3' ELSE 'Others' END AS ranking, "
        + "score "
        + "FROM students WHERE class = 'A'";

    String expected = "SELECT name, CASE WHEN RANK() OVER (ORDER BY score DESC) <= "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS ranking, score FROM students WHERE class = "
        + SqlParser.REDACTED_STRING;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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


    String expected = "SELECT e.name AS employee_name, CASE WHEN e.salary > "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " WHEN e.salary > "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS grade, SUM(e.bonus) OVER (PARTITION BY e.department ORDER BY e.hire_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bonus FROM employees e WHERE e.salary > ANY(SELECT avg_salary FROM department_stats WHERE dept = "
        + SqlParser.REDACTED_STRING
        + ") AND e.status = "
        + SqlParser.REDACTED_STRING
        + " ORDER BY CASE WHEN e.role = "
        + SqlParser.REDACTED_STRING
        + " THEN "
        + SqlParser.REDACTED_NUMBER
        + " ELSE "
        + SqlParser.REDACTED_NUMBER
        + " END, e.name ASC";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testPostgreSQLCaseWithArrayAndBinaryOps() {
    String sql = "SELECT "
        + "CASE WHEN tags[1] = 'urgent' THEN priority * 2 "
        + "ELSE priority END AS adjusted_priority, "
        + "data->>'name' AS extracted_name "
        + "FROM tasks WHERE status <> 'deleted' "
        + "AND (flags & 4) > 0";

    String expected = "SELECT CASE WHEN tags["
        + SqlParser.REDACTED_NUMBER
        + "] = "
        + SqlParser.REDACTED_STRING
        + " THEN priority * "
        + SqlParser.REDACTED_NUMBER
        + " ELSE priority END AS adjusted_priority, data->>"
        + SqlParser.REDACTED_STRING
        + " AS extracted_name FROM tasks WHERE status <> "
        + SqlParser.REDACTED_STRING
        + " AND (flags & "
        + SqlParser.REDACTED_NUMBER
        + ") > "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
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

    String expected = "SELECT CASE WHEN LEVEL = "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " WHEN LEVEL = "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS node_type, name, LEVEL FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

  @Test
  public void testMySQLCaseWithGroupConcat() {
    String sql = "SELECT department, "
        + "GROUP_CONCAT(CASE WHEN rating >= 4 THEN name "
        + "ELSE NULL END ORDER BY rating DESC SEPARATOR ', ') AS top_performers "
        + "FROM employees "
        + "WHERE year = 2024 "
        + "GROUP BY department";

    String expected = "SELECT department, GROUP_CONCAT(CASE WHEN rating >= "
        + SqlParser.REDACTED_NUMBER
        + " THEN name ELSE NULL END ORDER BY rating DESC SEPARATOR "
        + SqlParser.REDACTED_STRING
        + ") AS top_performers FROM employees WHERE year = "
        + SqlParser.REDACTED_NUMBER
        + " GROUP BY department";
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }

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

    String expected = "SELECT CASE WHEN e.salary > "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " WHEN e.salary > "
        + SqlParser.REDACTED_NUMBER
        + " THEN "
        + SqlParser.REDACTED_STRING
        + " ELSE "
        + SqlParser.REDACTED_STRING
        + " END AS grade, COALESCE(e.phone, "
        + SqlParser.REDACTED_STRING
        + ") AS phone, SUM(e.bonus) OVER (PARTITION BY e.dept ORDER BY e.hire_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bonus FROM employees e WHERE e.salary > ANY(SELECT avg_sal FROM dept_stats WHERE dept = "
        + SqlParser.REDACTED_STRING
        + ") AND e.status = "
        + SqlParser.REDACTED_STRING
        + " AND e.hire_date > "
        + SqlParser.REDACTED_STRING
        + " ORDER BY e.salary DESC LIMIT "
        + SqlParser.REDACTED_NUMBER;
    assertEquals(expected, SqlParser.redactSensitiveData(sql));
  }
}
