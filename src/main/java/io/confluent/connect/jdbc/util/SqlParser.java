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
import net.sf.jsqlparser.expression.AllValue;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnalyticType;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.ConnectByRootOperator;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonAggregateFunction;
import net.sf.jsqlparser.expression.JsonAggregateOnNullType;
import net.sf.jsqlparser.expression.JsonAggregateUniqueKeysType;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.JsonFunction;
import net.sf.jsqlparser.expression.JsonFunctionExpression;
import net.sf.jsqlparser.expression.JsonFunctionType;
import net.sf.jsqlparser.expression.JsonKeyValuePair;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.OracleNamedFunctionParameter;
import net.sf.jsqlparser.expression.OverlapsCondition;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RangeExpression;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.RowGetExpression;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.TimezoneExpression;
import net.sf.jsqlparser.expression.TranscodingFunction;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.VariableAssignment;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WindowDefinition;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.expression.WindowOffset;
import net.sf.jsqlparser.expression.WindowRange;
import net.sf.jsqlparser.expression.XMLSerializeExpr;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.FullTextSearch;
import net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlParser {

  private static final Logger log = LoggerFactory.getLogger(SqlParser.class);

  public static final String REDACTED_STRING = "'********'";
  public static final String REDACTED_NUMBER = "0";
  private static final String REDACTED_VALUE = "<redacted>";

  /**
   * Regex pattern to match SQL string literals.
   * Handles both standard SQL escaping ('') and MySQL backslash escaping (\').
   * This pattern is used as a safety net after AST-based redaction to catch any
   * string literals that may have leaked through expression types that bypass
   * the visitor pattern (e.g., JsonFunction in JSqlParser 4.9).
   */
  static final Pattern STRING_LITERAL_PATTERN =
      Pattern.compile("'(?:[^'\\\\]|\\\\.|'')*'");

  /**
   * Regex pattern to match PostgreSQL dollar-quoted string literals.
   * Dollar-quoting uses the syntax {@code $$content$$} or {@code $tag$content$tag$}
   * where the tag is an optional identifier. JSqlParser 4.9 parses these but does NOT
   * route them through the ExpressionVisitor as StringValue nodes, so they bypass
   * Layer 1 (AST-based redaction) entirely. This pattern is applied in Layers 2, 3,
   * and 4 to ensure dollar-quoted PII is always caught.
   *
   * <p>The pattern matches: {@code $} + optional tag (word chars) + {@code $} +
   * content (non-greedy) + {@code $} + same tag + {@code $}.</p>
   */
  static final Pattern DOLLAR_QUOTED_PATTERN =
      Pattern.compile("\\$(\\w*)\\$.*?\\$\\1\\$", Pattern.DOTALL);

  /**
   * Regex pattern to match standalone numeric literals (integers and decimals).
   * Uses lookbehind/lookahead to avoid matching numbers that are part of identifiers
   * (e.g., 'table1', 'col_2'). This pattern is used only as a fallback when
   * AST parsing fails entirely, to preserve query structure while redacting values.
   */
  static final Pattern NUMERIC_LITERAL_PATTERN =
      Pattern.compile("(?<![a-zA-Z_$\\d])\\d+(?:\\.\\d+)?(?![a-zA-Z_$\\d])");

  /**
   * Patterns identifying structural numeric positions in SQL output where numbers
   * represent SQL syntax elements (not data values) and should not trigger leak
   * detection. These cover clauses where JSqlParser's SelectDeParser serializes
   * numbers directly without going through the ExpressionVisitor.
   *
   * <p>Confirmed structural (bypass visitor): TOP, OPTIMIZE FOR, type definitions.
   * Note: LIMIT, OFFSET, FETCH go through the visitor and are redacted to 0.</p>
   */
  static final Pattern[] STRUCTURAL_NUMBER_PATTERNS = {
      // SQL Server TOP clause: TOP n or TOP n PERCENT
      Pattern.compile("(?i)\\bTOP\\s+(\\d+(?:\\.\\d+)?)"),
      // DB2 OPTIMIZE FOR clause: OPTIMIZE FOR n ROWS
      Pattern.compile("(?i)\\bOPTIMIZE\\s+FOR\\s+(\\d+(?:\\.\\d+)?)"),
  };

  /**
   * Pattern for SQL type definitions with numeric parameters.
   * These parameters (precision, scale, length) are type metadata, not data values.
   * Handles types like DECIMAL(10, 2), VARCHAR(255), NUMBER(10), etc.
   */
  static final Pattern TYPE_DEF_PARAMS_PATTERN = Pattern.compile(
      "(?i)\\b(?:DECIMAL|NUMERIC|VARCHAR|NVARCHAR|CHAR|NCHAR|FLOAT|REAL|"
      + "NUMBER|BINARY|VARBINARY|BIT|DATETIME2|DATETIMEOFFSET|TIME|TIMESTAMP|"
      + "RAW|CLOB|BLOB|NCLOB)\\s*\\(([^)]+)\\)"
  );

  /**
   * Simple pattern to find numbers within type definition parameter strings.
   */
  private static final Pattern INNER_NUMBER_PATTERN =
      Pattern.compile("\\d+(?:\\.\\d+)?");

  /**
   * Redacts sensitive data (PII) from a SQL query while preserving the query structure.
   * Uses a multi-layered approach:
   * <ol>
   *   <li><b>Layer 1 (AST-based):</b> Parses the SQL using JSqlParser and replaces all
   *       literal values (strings, numbers, dates, etc.) with redaction markers via the
   *       visitor pattern.</li>
   *   <li><b>Layer 2 (Regex safety net):</b> After AST processing, applies regex patterns to
   *       catch any remaining string literals that may have leaked through expression types
   *       that bypass the JSqlParser visitor pattern. This covers both single-quoted literals
   *       and PostgreSQL dollar-quoted literals ({@code $$...$$}, {@code $tag$...$tag$}).</li>
   *   <li><b>Layer 3 (Post-redaction verification):</b> Scans the redacted output for any
   *       unredacted string or numeric literals. String literals must all be '********'.
   *       Numeric literals must be 0 (redaction marker) or in a known structural context
   *       (TOP, OPTIMIZE FOR, type definitions). If any unredacted literal is detected,
   *       the entire query is fully redacted for safety.</li>
   *   <li><b>Layer 4 (Regex fallback):</b> When AST parsing fails entirely (e.g., for SQL
   *       syntax not supported by JSqlParser), applies regex-based redaction of both string
   *       and numeric literals to preserve query structure while removing PII.</li>
   * </ol>
   *
   * @param sql the SQL query to redact
   * @return the redacted SQL query, or {@code "<redacted>"} if the query cannot be processed,
   *         or an empty string if the input is null/empty
   */
  public static String redactSensitiveData(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return "";
    }

    try {
      // Layer 1: AST-based redaction using JSqlParser visitor pattern
      Statement statement = CCJSqlParserUtil.parse(sql);
      StringBuilder buffer = new StringBuilder();

      ExpressionDeParser expressionDeParser = new RedactingExpressionDeParser();
      SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, buffer);
      expressionDeParser.setSelectVisitor(selectDeParser);
      expressionDeParser.setBuffer(buffer);

      StatementDeParser statementDeParser = new StatementDeParser(
          expressionDeParser, selectDeParser, buffer);
      statement.accept(statementDeParser);

      String result = buffer.toString();

      // Layer 2: Regex safety net for any string literals that leaked through
      // expression types that bypass the visitor pattern. This is idempotent
      // for already-redacted values ('********' -> '********').
      result = redactStringLiterals(result);

      // Layer 3: Post-redaction verification — ensure no unredacted literals
      // remain in the output. If any are detected, fully redact for safety.
      if (hasLeakedLiterals(result)) {
        log.warn("Detected potential unredacted literals after AST processing, "
            + "fully redacting query for safety");
        return REDACTED_VALUE;
      }

      return result;
    } catch (JSQLParserException e) {
      // Layer 4: When AST parsing fails (unsupported SQL syntax/functions),
      // apply regex-based redaction to still preserve query structure
      log.debug("JSqlParser could not parse the SQL query, falling back to regex redaction", e);
      return redactAllLiterals(sql);
    }
  }

  /**
   * Redacts all string literals in the SQL using regex, including both standard
   * single-quoted literals and PostgreSQL dollar-quoted literals ({@code $$...$$},
   * {@code $tag$...$tag$}). Used as a safety net after AST-based redaction to catch
   * any literals that leaked through expression types that bypass the JSqlParser
   * visitor pattern.
   *
   * <p>Dollar-quoted strings are redacted first to prevent interference with the
   * single-quote pattern (dollar-quoted content may itself contain single quotes).</p>
   *
   * @param sql the SQL string to process
   * @return the SQL with all string literals replaced with the redaction marker
   */
  static String redactStringLiterals(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    // Redact dollar-quoted strings first (they may contain single quotes)
    String result = DOLLAR_QUOTED_PATTERN.matcher(sql).replaceAll(REDACTED_STRING);
    // Then redact standard single-quoted strings
    return STRING_LITERAL_PATTERN.matcher(result).replaceAll(REDACTED_STRING);
  }

  /**
   * Redacts both string literals and numeric literals using regex.
   * Used as a fallback when AST parsing fails entirely, to preserve query structure
   * (keywords, function names, table/column names) while removing all literal values.
   *
   * @param sql the SQL string to process
   * @return the SQL with all literals replaced, or {@code "<redacted>"} if the input is
   *         null/empty or processing fails
   */
  static String redactAllLiterals(String sql) {
    if (sql == null || sql.isEmpty()) {
      return REDACTED_VALUE;
    }
    try {
      // First, redact dollar-quoted strings (they may contain single quotes)
      String result = DOLLAR_QUOTED_PATTERN.matcher(sql).replaceAll(REDACTED_STRING);
      // Then, redact single-quoted string literals
      result = STRING_LITERAL_PATTERN.matcher(result).replaceAll(REDACTED_STRING);
      // Finally, redact standalone numeric literals
      result = NUMERIC_LITERAL_PATTERN.matcher(result).replaceAll(REDACTED_NUMBER);
      return result;
    } catch (Exception e) {
      // If regex processing fails for any reason, fully redact
      return REDACTED_VALUE;
    }
  }

  /**
   * Post-redaction verification: checks whether any literal values leaked through
   * the AST-based redaction process (Layer 1) and the regex safety net (Layer 2).
   *
   * <p><b>Dollar-quoted verification:</b> Scans for any remaining PostgreSQL
   * dollar-quoted strings ({@code $$...$$}, {@code $tag$...$tag$}). These bypass
   * the JSqlParser visitor pattern entirely and must be caught here if Layer 2
   * did not remove them.</p>
   *
   * <p><b>String verification:</b> Scans the redacted output for any single-quoted string
   * that is not the redaction marker ('********'). This check has zero false positives
   * since all string literals should have been redacted.</p>
   *
   * <p><b>Numeric verification:</b> Scans the redacted output for any standalone number
   * that is not the redaction marker (0) and is not in a known structural SQL context.
   * Structural contexts include TOP n, OPTIMIZE FOR n, and type definition parameters
   * like DECIMAL(10, 2). Numbers in these positions are SQL syntax elements, not PII.</p>
   *
   * @param redactedSql the SQL query after Layer 1 and Layer 2 redaction
   * @return true if potential leaked literals are detected, false if verification passes
   */
  static boolean hasLeakedLiterals(String redactedSql) {
    if (redactedSql == null || redactedSql.isEmpty()) {
      return false;
    }

    // Check 0: Verify no dollar-quoted strings remain in the output.
    // After Layer 2, all $$...$$ and $tag$...$tag$ should be redacted.
    if (DOLLAR_QUOTED_PATTERN.matcher(redactedSql).find()) {
      return true;
    }

    // Check 1: Verify all string literals in the output are properly redacted.
    // After Layer 2, every '...' should be '********'.
    Matcher stringMatcher = STRING_LITERAL_PATTERN.matcher(redactedSql);
    while (stringMatcher.find()) {
      if (!REDACTED_STRING.equals(stringMatcher.group())) {
        return true;
      }
    }

    // Check 2: Verify all standalone numbers are either the redaction marker (0)
    // or in known structural positions (TOP, OPTIMIZE FOR, type definitions).
    Set<Integer> structuralPositions = findStructuralNumberPositions(redactedSql);
    Matcher numMatcher = NUMERIC_LITERAL_PATTERN.matcher(redactedSql);
    while (numMatcher.find()) {
      String numStr = numMatcher.group();
      // Skip the redaction marker value (0 or 0.0, 0.00, etc.)
      try {
        if (Double.parseDouble(numStr) == 0.0) {
          continue;
        }
      } catch (NumberFormatException e) {
        // Not a valid number; treat as potential leak
      }
      // Skip numbers at known structural positions
      if (structuralPositions.contains(numMatcher.start())) {
        continue;
      }
      // Non-zero, non-structural number found — potential leak
      return true;
    }

    return false;
  }

  /**
   * Identifies positions of numbers in the SQL string that are part of known
   * structural SQL clauses (not data values). These positions are excluded from
   * the leak detection verification.
   *
   * <p>Structural contexts include:</p>
   * <ul>
   *   <li>SQL Server TOP clause: {@code TOP 10}, {@code TOP 25 PERCENT}</li>
   *   <li>DB2 OPTIMIZE FOR clause: {@code OPTIMIZE FOR 100 ROWS}</li>
   *   <li>Type definition parameters: {@code DECIMAL(10, 2)}, {@code VARCHAR(255)}</li>
   * </ul>
   *
   * <p>Note: LIMIT, OFFSET, and FETCH clauses are NOT included here because their
   * numbers go through the JSqlParser ExpressionVisitor and are already redacted
   * to 0 by Layer 1.</p>
   *
   * @param sql the SQL string to scan for structural number positions
   * @return a set of character positions where structural numbers start
   */
  private static Set<Integer> findStructuralNumberPositions(String sql) {
    Set<Integer> positions = new HashSet<>();

    // Check clause-level structural patterns (TOP, OPTIMIZE FOR)
    for (Pattern pattern : STRUCTURAL_NUMBER_PATTERNS) {
      Matcher matcher = pattern.matcher(sql);
      while (matcher.find()) {
        for (int g = 1; g <= matcher.groupCount(); g++) {
          if (matcher.group(g) != null) {
            positions.add(matcher.start(g));
          }
        }
      }
    }

    // Check type definition parameters: DECIMAL(10, 2), VARCHAR(255), etc.
    Matcher typeDefMatcher = TYPE_DEF_PARAMS_PATTERN.matcher(sql);
    while (typeDefMatcher.find()) {
      String params = typeDefMatcher.group(1);
      int paramsStart = typeDefMatcher.start(1);
      Matcher numInType = INNER_NUMBER_PATTERN.matcher(params);
      while (numInType.find()) {
        positions.add(paramsStart + numInType.start());
      }
    }

    return positions;
  }

  /**
   * The core redacting expression visitor. Provides explicit override methods for ALL
   * expression types in JSqlParser 4.9's ExpressionVisitor interface, ensuring complete
   * coverage for PII redaction across all database dialects (PostgreSQL, MySQL, Oracle,
   * SQL Server, DB2, etc.).
   *
   * <p><b>Design principle:</b> Every expression type is explicitly overridden to document
   * that it has been security-audited. This ensures that any future changes to the base
   * ExpressionDeParser class cannot inadvertently leak PII data.</p>
   *
   * <h3>Category 1: Literal value overrides (replace data with redaction markers)</h3>
   * <ul>
   *   <li>String literals: {@link StringValue} → {@code '********'}</li>
   *   <li>Numeric literals: {@link LongValue}, {@link DoubleValue} → {@code 0}</li>
   *   <li>Hex literals: {@link HexValue} → {@code '********'}</li>
   *   <li>Date/time literals: {@link DateValue}, {@link TimestampValue},
   *       {@link TimeValue}, {@link DateTimeLiteralExpression} → {@code '********'}</li>
   *   <li>NULL keyword: {@link NullValue} → preserved as {@code NULL} (structural)</li>
   *   <li>Parameters: {@link JdbcParameter} → {@code '********'}</li>
   *   <li>Signed values: {@link SignedExpression} → redacted when wrapping literals</li>
   * </ul>
   *
   * <h3>Category 2: toString()-bypass overrides (PII risk — custom traversal)</h3>
   * <p>These expression types use toString() or append(Object) in the base
   * ExpressionDeParser, bypassing the visitor pattern. We override with custom
   * traversal to ensure sub-expressions are visited for redaction.</p>
   * <ul>
   *   <li>JSON: {@link JsonFunction}, {@link JsonAggregateFunction},
   *       {@link JsonExpression} — bypass via append(buffer)/toString()</li>
   *   <li>Search: {@link FullTextSearch} — bypass via string concatenation</li>
   *   <li>Relational: {@link IsDistinctExpression} — bypass via string concatenation</li>
   *   <li>MySQL: {@link MySQLGroupConcat} — bypass via toString()</li>
   *   <li>Oracle: {@link OracleHierarchicalExpression},
   *       {@link KeepExpression} — bypass via toString()</li>
   *   <li>Conditions: {@link OverlapsCondition} — bypass via toString()</li>
   *   <li>Functions: {@link Function} (KEEP bypass), {@link AnalyticExpression}
   *       (window/ORDER BY bypass), {@link XMLSerializeExpr}
   *       (ORDER BY bypass)</li>
   *   <li>Compound: {@link CollateExpression} (left expression bypass),
   *       {@link IntervalExpression} (parameter path bypass)</li>
   * </ul>
   *
   * <h3>Category 3: Structural toString()-bypass overrides (no PII — audit documented)</h3>
   * <p>These use toString() in the base class but contain only structural SQL elements.
   * Overridden for defense-in-depth and to document the security audit.</p>
   * <ul>
   *   <li>{@link JdbcNamedParameter} — {@code :paramName}</li>
   *   <li>{@link UserVariable} — {@code @varName}</li>
   *   <li>{@link NumericBind} — {@code :1}</li>
   *   <li>{@link OracleHint} — {@code /*+ hint * /}</li>
   *   <li>{@link TimeKeyExpression} — {@code CURRENT_TIMESTAMP}</li>
   *   <li>{@link NextValExpression} — {@code NEXT VALUE FOR seq}</li>
   *   <li>{@link AllColumns} — {@code *}</li>
   *   <li>{@link AllTableColumns} — {@code t.*}</li>
   *   <li>{@link AllValue} — {@code ALL}</li>
   * </ul>
   *
   * <h3>Category 4: Visitor-safe overrides (defense-in-depth)</h3>
   * <p>These properly use accept() in the base class. Overridden explicitly to
   * document the audit and guard against future base class changes.</p>
   * <ul>
   *   <li>Conditional: {@link CaseExpression} (CASE...WHEN...THEN...ELSE...END),
   *       {@link WhenClause} (individual WHEN...THEN pairs)</li>
   *   <li>Subquery: {@link AnyComparisonExpression} (ANY/SOME/ALL subquery
   *       comparisons, uses {@link net.sf.jsqlparser.expression.AnyType} enum)</li>
   *   <li>Array: {@link ArrayExpression}, {@link ArrayConstructor}</li>
   *   <li>Oracle: {@link ConnectByRootOperator},
   *       {@link OracleNamedFunctionParameter}</li>
   *   <li>Functions: {@link CastExpression}, {@link ExtractExpression},
   *       {@link TranscodingFunction}, {@link TrimFunction}</li>
   *   <li>Structural: {@link NotExpression}, {@link Parenthesis},
   *       {@link RowConstructor}, {@link RowGetExpression},
   *       {@link VariableAssignment}, {@link RangeExpression},
   *       {@link TimezoneExpression}</li>
   * </ul>
   *
   * <h3>Category 5: Visitor-safe types using base class (no override needed)</h3>
   * <p>These types properly use accept()/visitBinaryExpression() in the base class.
   * Sub-expressions flow through the visitor and hit our literal value overrides.
   * Note: {@link net.sf.jsqlparser.expression.BinaryExpression} is the abstract base
   * class for all binary operators — it is not directly visitable; each concrete
   * subclass has its own visit() method.</p>
   * <ul>
   *   <li>Arithmetic: Addition, Subtraction, Multiplication, Division,
   *       IntegerDivision, Modulo, Concat, BitwiseAnd/Or/Xor/LeftShift/RightShift</li>
   *   <li>Conditional: AndExpression, OrExpression, XorExpression</li>
   *   <li>Comparison: EqualsTo, NotEqualsTo, GreaterThan, GreaterThanEquals,
   *       MinorThan, MinorThanEquals, Between, InExpression, LikeExpression,
   *       SimilarToExpression, RegExpMatchOperator, JsonOperator, Contains,
   *       ContainedBy, DoubleAnd, Matches, GeometryDistance, TSQLLeftJoin,
   *       TSQLRightJoin</li>
   *   <li>Predicates: IsNullExpression, IsBooleanExpression, ExistsExpression,
   *       MemberOfExpression</li>
   *   <li>Structural: Column, Select, ParenthesedSelect, ExpressionList</li>
   * </ul>
   *
   * <h3>Category 6: Non-Expression helper classes (security-audited, no visit needed)</h3>
   * <p>These classes from the expression package do NOT implement Expression and are
   * NOT part of ExpressionVisitor. They are helper/support classes used by other
   * expression types or by the statement/select deparsers. Each has been audited:</p>
   * <ul>
   *   <li>{@link net.sf.jsqlparser.expression.Alias} — column/table alias names
   *       (structural metadata, not PII)</li>
   *   <li>{@link net.sf.jsqlparser.expression.AnyType} — enum (ANY, SOME, ALL)
   *       for {@link AnyComparisonExpression}</li>
   *   <li>{@link net.sf.jsqlparser.expression.BinaryExpression} — abstract base
   *       for binary operators; concrete subclasses handled individually</li>
   *   <li>{@link net.sf.jsqlparser.expression.ExpressionVisitor} — the visitor
   *       interface we implement through ExpressionDeParser</li>
   *   <li>{@link net.sf.jsqlparser.expression.ExpressionVisitorAdapter} — default
   *       visitor adapter; we extend ExpressionDeParser instead</li>
   *   <li>{@link net.sf.jsqlparser.expression.FilterOverImpl} — FILTER/OVER clause
   *       helper; handled through our {@link AnalyticExpression} override</li>
   *   <li>{@link net.sf.jsqlparser.expression.MySQLIndexHint} — MySQL index hints
   *       (USE/FORCE/IGNORE INDEX); structural optimizer directives</li>
   *   <li>{@link net.sf.jsqlparser.expression.OrderByClause} — ORDER BY helper;
   *       handled through our visitOrderByElements() method</li>
   *   <li>{@link net.sf.jsqlparser.expression.PartitionByClause} — PARTITION BY
   *       helper; handled through our visitExpressionList() method</li>
   *   <li>{@link net.sf.jsqlparser.expression.SQLServerHints} — SQL Server table
   *       hints (WITH (NOLOCK), INDEX); structural optimizer directives</li>
   * </ul>
   *
   * <h3>Database-specific function coverage</h3>
   * <p>Most database-specific functions (e.g., PostgreSQL's {@code COALESCE},
   * {@code NULLIF}, {@code ARRAY_AGG}; MySQL's {@code IFNULL}, {@code DATE_FORMAT},
   * {@code CONCAT_WS}; Oracle's {@code NVL}, {@code TO_DATE}, {@code DECODE};
   * SQL Server's {@code ISNULL}, {@code CONVERT}, {@code CHARINDEX}, {@code LEN},
   * {@code DATEDIFF}, {@code FORMAT}) are parsed by JSqlParser as generic
   * {@link Function} nodes. The Function visit method traverses all parameters
   * through the visitor pattern, so these functions are automatically handled
   * by our literal value overrides without needing individual overrides.</p>
   */
  private static class RedactingExpressionDeParser extends ExpressionDeParser {

    // =========================================================================
    // Literal value overrides — replace all data literals with redaction markers
    // =========================================================================

    @Override
    public void visit(StringValue stringValue) {
      getBuffer().append(REDACTED_STRING);
    }

    @Override
    public void visit(LongValue longValue) {
      getBuffer().append(REDACTED_NUMBER);
    }

    @Override
    public void visit(DoubleValue doubleValue) {
      getBuffer().append(REDACTED_NUMBER);
    }

    @Override
    public void visit(HexValue hexValue) {
      getBuffer().append(REDACTED_STRING);
    }

    @Override
    public void visit(DateValue dateValue) {
      getBuffer().append(REDACTED_STRING);
    }

    @Override
    public void visit(TimestampValue timestampValue) {
      getBuffer().append(REDACTED_STRING);
    }

    @Override
    public void visit(TimeValue timeValue) {
      getBuffer().append(REDACTED_STRING);
    }

    @Override
    public void visit(DateTimeLiteralExpression dateTimeLiteral) {
      getBuffer().append(REDACTED_STRING);
    }

    @Override
    public void visit(NullValue nullValue) {
      // NullValue is the SQL NULL keyword — structural, not PII.
      // Explicitly overridden to avoid the base class toString() bypass
      // and to document that this type has been audited.
      getBuffer().append("NULL");
    }

    // =========================================================================
    // Parameter and placeholder overrides
    // =========================================================================

    @Override
    public void visit(JdbcParameter jdbcParameter) {
      getBuffer().append(REDACTED_STRING);
    }

    /**
     * Array access expression: {@code column[index]} or {@code column[start:stop]}.
     * The base class already uses accept() for sub-expressions, but we override
     * explicitly to document the audit and ensure proper visitor traversal.
     * Note: Array literals like {@code ARRAY['a', 'b']} are handled by
     * {@code ArrayConstructor}, not this class.
     */
    @Override
    public void visit(ArrayExpression arrayExpression) {
      arrayExpression.getObjExpression().accept(this);
      getBuffer().append("[");
      if (arrayExpression.getIndexExpression() != null) {
        arrayExpression.getIndexExpression().accept(this);
      } else {
        if (arrayExpression.getStartIndexExpression() != null) {
          arrayExpression.getStartIndexExpression().accept(this);
        }
        getBuffer().append(":");
        if (arrayExpression.getStopIndexExpression() != null) {
          arrayExpression.getStopIndexExpression().accept(this);
        }
      }
      getBuffer().append("]");
    }

    // =========================================================================
    // Compound expression overrides — traverse sub-expressions via visitor
    // =========================================================================

    @Override
    public void visit(SignedExpression signedExpression) {
      if (signedExpression.getExpression() instanceof LongValue
          || signedExpression.getExpression() instanceof DoubleValue) {
        getBuffer().append(REDACTED_NUMBER);
      } else {
        getBuffer().append(signedExpression.getSign());
        signedExpression.getExpression().accept(this);
      }
    }

    @Override
    public void visit(CollateExpression collateExpression) {
      collateExpression.getLeftExpression().accept(this);
      getBuffer().append(" COLLATE ");
      getBuffer().append(collateExpression.getCollate());
    }

    /**
     * Interval expression: {@code INTERVAL value type} (e.g., {@code INTERVAL 30 DAY}).
     * The base class has two paths:
     * <ul>
     *   <li>Expression path: visits sub-expression via accept() — our literal overrides
     *       handle redaction.</li>
     *   <li>Parameter path: uses getParameter() which returns a string via toString()
     *       bypass — we redact this directly.</li>
     * </ul>
     */
    @Override
    public void visit(IntervalExpression intervalExpression) {
      if (intervalExpression.isUsingIntervalKeyword()) {
        getBuffer().append("INTERVAL ");
      }
      if (intervalExpression.getExpression() != null) {
        // Expression path: sub-expression goes through visitor (handles redaction)
        intervalExpression.getExpression().accept(this);
      } else {
        // Parameter path: string value bypasses visitor — redact directly
        getBuffer().append(REDACTED_NUMBER);
      }
      if (intervalExpression.getIntervalType() != null) {
        getBuffer().append(" ");
        getBuffer().append(intervalExpression.getIntervalType());
      }
    }

    // =========================================================================
    // Structural toString()-bypass overrides — these expression types use
    // toString() in the base ExpressionDeParser but contain only structural
    // SQL elements (no PII data). We override explicitly to:
    //   1. Document that each type has been security-audited
    //   2. Future-proof against base class changes that might introduce PII
    //   3. Ensure complete visitor pattern coverage
    // =========================================================================

    /**
     * JDBC named parameter: {@code :paramName}.
     * Base class uses toString() bypass. Output is a parameter placeholder
     * name — structural, not PII. Overridden for audit documentation.
     */
    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
      getBuffer().append(jdbcNamedParameter.toString());
    }

    /**
     * User variable: {@code @varName} (MySQL) or {@code @@sysVar}.
     * Base class uses toString() bypass. Output is a variable name —
     * structural, not PII. Overridden for audit documentation.
     */
    @Override
    public void visit(UserVariable userVariable) {
      getBuffer().append(userVariable.toString());
    }

    /**
     * Numeric bind variable: {@code :1}, {@code :2} (Oracle positional binds).
     * Base class uses toString() bypass. Output is a bind position —
     * structural, not PII. Overridden for audit documentation.
     */
    @Override
    public void visit(NumericBind numericBind) {
      getBuffer().append(numericBind.toString());
    }

    /**
     * Oracle optimizer hint: {@code /*+ FIRST_ROWS(10) * /}.
     * Base class uses toString() bypass. Output is an optimizer directive —
     * structural, not PII. Numbers within hints (e.g., FIRST_ROWS(10)) are
     * optimizer parameters, not data values.
     * Overridden for audit documentation.
     */
    @Override
    public void visit(OracleHint oracleHint) {
      getBuffer().append(oracleHint.toString());
    }

    /**
     * Time keyword expression: {@code CURRENT_TIMESTAMP}, {@code CURRENT_DATE},
     * {@code CURRENT_TIME}, {@code SYSDATE}.
     * Base class uses toString() bypass. Output is a SQL keyword —
     * structural, not PII. Overridden for audit documentation.
     */
    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
      getBuffer().append(timeKeyExpression.toString());
    }

    /**
     * Sequence next value: {@code NEXT VALUE FOR seq_name} or {@code NEXTVAL FOR seq_name}.
     * Base class uses getName() which returns the sequence name — structural,
     * not PII. Overridden for audit documentation and explicit traversal.
     */
    @Override
    public void visit(NextValExpression nextValExpression) {
      getBuffer().append(
          nextValExpression.isUsingNextValueFor() ? "NEXT VALUE FOR " : "NEXTVAL FOR "
      );
      getBuffer().append(nextValExpression.getName());
    }

    /**
     * All columns wildcard: {@code *}.
     * Base class uses toString() bypass. Output is the wildcard —
     * structural, not PII. Overridden for audit documentation.
     */
    @Override
    public void visit(AllColumns allColumns) {
      getBuffer().append(allColumns.toString());
    }

    /**
     * Table-qualified all columns: {@code table.*}.
     * Base class uses toString() bypass. Output is a qualified wildcard —
     * structural, not PII. Overridden for audit documentation.
     */
    @Override
    public void visit(AllTableColumns allTableColumns) {
      getBuffer().append(allTableColumns.toString());
    }

    /**
     * ALL keyword in subquery comparisons: {@code > ALL (subquery)}.
     * Base class uses append(Object) which calls toString(). Output is
     * the ALL keyword — structural, not PII.
     * Overridden for audit documentation.
     */
    @Override
    public void visit(AllValue allValue) {
      getBuffer().append(allValue.toString());
    }

    // =========================================================================
    // Visitor-safe expression overrides — these expression types properly use
    // accept() in the base ExpressionDeParser, but we override explicitly
    // for defense-in-depth and audit documentation. Sub-expressions are
    // visited through the visitor, so our literal overrides handle redaction.
    // =========================================================================

    /**
     * CASE expression: {@code CASE [switch] WHEN cond THEN result ... [ELSE default] END}.
     * Base class properly visits switchExpression, whenClauses, and elseExpression
     * via accept(). Overridden explicitly for defense-in-depth, ensuring that all
     * WHEN condition values (which often contain PII such as
     * {@code WHEN status = 'active'}) and THEN result values are traversed through
     * the visitor for redaction.
     *
     * <p>Related: {@link WhenClause} — each individual WHEN...THEN pair.
     * Also related: {@link net.sf.jsqlparser.expression.AnyType} — enum used by
     * {@link AnyComparisonExpression}, not related to CASE.</p>
     */
    @Override
    public void visit(CaseExpression caseExpression) {
      if (caseExpression.isUsingBrackets()) {
        getBuffer().append("(");
      }
      getBuffer().append("CASE ");
      Expression switchExpr = caseExpression.getSwitchExpression();
      if (switchExpr != null) {
        switchExpr.accept(this);
        getBuffer().append(" ");
      }
      if (caseExpression.getWhenClauses() != null) {
        for (WhenClause whenClause : caseExpression.getWhenClauses()) {
          whenClause.accept(this);
        }
      }
      Expression elseExpr = caseExpression.getElseExpression();
      if (elseExpr != null) {
        getBuffer().append("ELSE ");
        elseExpr.accept(this);
        getBuffer().append(" ");
      }
      getBuffer().append("END");
      if (caseExpression.isUsingBrackets()) {
        getBuffer().append(")");
      }
    }

    /**
     * WHEN clause: {@code WHEN condition THEN result}.
     * Part of a {@link CaseExpression}. Base class properly visits both
     * whenExpression and thenExpression via accept(). Overridden explicitly to
     * ensure PII values in WHEN conditions (e.g., {@code WHEN name = 'John'})
     * and THEN results (e.g., {@code THEN 'active'}) are always traversed
     * through the visitor for redaction.
     *
     * <p>Note: This is a concrete Expression type listed in the
     * {@link net.sf.jsqlparser.expression.ExpressionVisitor} interface.
     * It is NOT the same as the abstract {@link net.sf.jsqlparser.expression.BinaryExpression}
     * which is the base for operator expressions like EqualsTo, GreaterThan, etc.</p>
     */
    @Override
    public void visit(WhenClause whenClause) {
      getBuffer().append("WHEN ");
      whenClause.getWhenExpression().accept(this);
      getBuffer().append(" THEN ");
      whenClause.getThenExpression().accept(this);
      getBuffer().append(" ");
    }

    /**
     * ANY/SOME/ALL subquery comparison: {@code expr > ANY (SELECT ...)},
     * {@code expr = ALL (SELECT ...)}, {@code expr IN SOME (SELECT ...)}.
     * Base class visits the subquery Select via accept(). The
     * {@link net.sf.jsqlparser.expression.AnyType} enum (ANY, SOME, ALL) provides
     * the keyword — structural, not PII. Overridden explicitly to ensure the
     * subquery is always traversed through the visitor for redaction of any
     * literal values within it.
     *
     * <p>Note: {@link net.sf.jsqlparser.expression.AnyType} is a simple enum and
     * does not implement Expression, so it needs no visit method.</p>
     */
    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
      getBuffer().append(anyComparisonExpression.getAnyType().name());
      getBuffer().append(" ");
      anyComparisonExpression.getSelect().accept(this);
    }

    /**
     * Array constructor: {@code ARRAY[expr1, expr2, ...]}.
     * Base class properly visits each element via accept(). Overridden to
     * document audit and ensure array literal values are redacted.
     */
    @Override
    public void visit(ArrayConstructor arrayConstructor) {
      if (arrayConstructor.isArrayKeyword()) {
        getBuffer().append("ARRAY");
      }
      getBuffer().append("[");
      boolean first = true;
      for (Expression expression : arrayConstructor.getExpressions()) {
        if (!first) {
          getBuffer().append(", ");
        } else {
          first = false;
        }
        expression.accept(this);
      }
      getBuffer().append("]");
    }

    /**
     * Oracle CONNECT_BY_ROOT operator: {@code CONNECT_BY_ROOT column}.
     * Base class properly visits the column via accept(). Overridden to
     * document audit.
     */
    @Override
    public void visit(ConnectByRootOperator connectByRootOperator) {
      getBuffer().append("CONNECT_BY_ROOT ");
      connectByRootOperator.getColumn().accept(this);
    }

    /**
     * Oracle named function parameter: {@code param_name => expression}.
     * Base class properly visits the expression via accept(). Overridden
     * to document audit and ensure parameter values are redacted.
     */
    @Override
    public void visit(OracleNamedFunctionParameter oracleNamedFunctionParameter) {
      getBuffer().append(oracleNamedFunctionParameter.getName());
      getBuffer().append(" => ");
      oracleNamedFunctionParameter.getExpression().accept(this);
    }

    /**
     * Timezone expression: {@code expr AT TIME ZONE 'timezone'}.
     * Base class properly visits sub-expressions via accept(). Overridden
     * to document audit and ensure timezone values are redacted.
     */
    @Override
    public void visit(TimezoneExpression timezoneExpression) {
      timezoneExpression.getLeftExpression().accept(this);
      for (Expression expr : timezoneExpression.getTimezoneExpressions()) {
        getBuffer().append(" AT TIME ZONE ");
        expr.accept(this);
      }
    }

    /**
     * Transcoding function: {@code CONVERT(expr USING charset)}.
     * Base class properly visits the expression via accept(). Overridden
     * to document audit and ensure expression values are redacted.
     */
    @Override
    public void visit(TranscodingFunction transcodingFunction) {
      getBuffer().append("CONVERT( ");
      transcodingFunction.getExpression().accept(this);
      getBuffer().append(" USING ");
      getBuffer().append(transcodingFunction.getTranscodingName());
      getBuffer().append(" )");
    }

    /**
     * TRIM function: {@code TRIM([LEADING|TRAILING|BOTH] expr FROM expr)}.
     * Base class properly visits sub-expressions via accept(). Overridden
     * to document audit and ensure trim values are redacted.
     */
    @Override
    public void visit(TrimFunction trimFunction) {
      getBuffer().append("Trim(");
      if (trimFunction.getTrimSpecification() != null) {
        getBuffer().append(" ");
        getBuffer().append(trimFunction.getTrimSpecification());
      }
      if (trimFunction.getExpression() != null) {
        getBuffer().append(" ");
        trimFunction.getExpression().accept(this);
      }
      if (trimFunction.getFromExpression() != null) {
        getBuffer().append(trimFunction.isUsingFromKeyword() ? " FROM " : ", ");
        trimFunction.getFromExpression().accept(this);
      }
      getBuffer().append(" )");
    }

    /**
     * Range expression: {@code start:end} (e.g., PostgreSQL array slicing).
     * Base class properly visits start/end via accept(). Overridden to
     * document audit.
     */
    @Override
    public void visit(RangeExpression rangeExpression) {
      rangeExpression.getStartExpression().accept(this);
      getBuffer().append(":");
      rangeExpression.getEndExpression().accept(this);
    }

    /**
     * Row constructor: {@code ROW(expr1, expr2, ...)} or {@code (expr1, expr2, ...)}.
     * Base class properly visits sub-expressions via accept(). Overridden
     * to document audit and ensure row values are redacted.
     */
    @Override
    public void visit(RowConstructor rowConstructor) {
      if (rowConstructor.getName() != null) {
        getBuffer().append(rowConstructor.getName());
      }
      getBuffer().append("(");
      boolean first = true;
      for (Object expr : rowConstructor) {
        if (!first) {
          getBuffer().append(", ");
        } else {
          first = false;
        }
        if (expr instanceof Expression) {
          ((Expression) expr).accept(this);
        }
      }
      getBuffer().append(")");
    }

    /**
     * Row get expression: {@code expr.column_name} (accessing a field from a row type).
     * Base class properly visits the expression via accept(). Overridden
     * to document audit.
     */
    @Override
    public void visit(RowGetExpression rowGetExpression) {
      rowGetExpression.getExpression().accept(this);
      getBuffer().append(".");
      getBuffer().append(rowGetExpression.getColumnName());
    }

    /**
     * Variable assignment: {@code @var := expression} or {@code @var = expression}.
     * Base class properly visits sub-expressions via accept(). Overridden
     * to document audit and ensure assigned values are redacted.
     */
    @Override
    public void visit(VariableAssignment variableAssignment) {
      variableAssignment.getVariable().accept(this);
      getBuffer().append(" ");
      getBuffer().append(variableAssignment.getOperation());
      getBuffer().append(" ");
      variableAssignment.getExpression().accept(this);
    }

    /**
     * NOT expression: {@code NOT expr} or {@code ! expr}.
     * Base class properly visits the inner expression via accept(). Overridden
     * to document audit.
     */
    @Override
    public void visit(NotExpression notExpression) {
      if (notExpression.isExclamationMark()) {
        getBuffer().append("! ");
      } else {
        getBuffer().append("NOT ");
      }
      notExpression.getExpression().accept(this);
    }

    /**
     * Parenthesized expression: {@code (expr)}.
     * Base class properly visits the inner expression via accept(). Overridden
     * to document audit.
     */
    @Override
    public void visit(Parenthesis parenthesis) {
      getBuffer().append("(");
      parenthesis.getExpression().accept(this);
      getBuffer().append(")");
    }

    /**
     * CAST expression: {@code CAST(expr AS type)} or {@code expr::type}.
     * Base class properly visits the left expression via accept(). The type
     * definition is structural (not PII). Overridden to document audit.
     */
    @Override
    public void visit(CastExpression castExpression) {
      if (castExpression.isUseCastKeyword()) {
        getBuffer().append(castExpression.keyword);
        getBuffer().append("(");
        castExpression.getLeftExpression().accept(this);
        getBuffer().append(" AS ");
        getBuffer().append(castExpression.getColDataType().toString());
        getBuffer().append(")");
      } else {
        castExpression.getLeftExpression().accept(this);
        getBuffer().append("::");
        getBuffer().append(castExpression.getColDataType());
      }
    }

    /**
     * EXTRACT function: {@code EXTRACT(part FROM expr)}.
     * Base class properly visits the expression via accept(). The part name
     * (YEAR, MONTH, etc.) is structural. Overridden to document audit.
     */
    @Override
    public void visit(ExtractExpression extractExpression) {
      getBuffer().append("EXTRACT(");
      getBuffer().append(extractExpression.getName());
      getBuffer().append(" FROM ");
      extractExpression.getExpression().accept(this);
      getBuffer().append(")");
    }

    // =========================================================================
    // JSON Function overrides — these bypass the visitor pattern in JSqlParser
    // 4.9's ExpressionDeParser by calling append(buffer)/toString() directly.
    // We override to properly traverse sub-expressions for redaction.
    // =========================================================================

    @Override
    public void visit(JsonFunction jsonFunction) {
      JsonFunctionType type = jsonFunction.getType();
      if (type == null) {
        getBuffer().append(REDACTED_STRING);
        return;
      }

      switch (type) {
        case OBJECT:
          visitJsonObject(jsonFunction);
          break;
        case POSTGRES_OBJECT:
          visitJsonPostgresObject(jsonFunction);
          break;
        case MYSQL_OBJECT:
          visitJsonMySqlObject(jsonFunction);
          break;
        case ARRAY:
          visitJsonArray(jsonFunction);
          break;
        default:
          getBuffer().append(REDACTED_STRING);
      }
    }

    @Override
    public void visit(JsonAggregateFunction jsonAggregateFunction) {
      JsonFunctionType type = jsonAggregateFunction.getType();
      if (type == null) {
        getBuffer().append(REDACTED_STRING);
        return;
      }

      switch (type) {
        case OBJECT:
          visitJsonObjectAgg(jsonAggregateFunction);
          break;
        case ARRAY:
          visitJsonArrayAgg(jsonAggregateFunction);
          break;
        default:
          getBuffer().append(REDACTED_STRING);
      }
    }

    /**
     * PostgreSQL JSON path expression: {@code column->>'key'}, {@code column->'key'->'nested'}.
     * Base class calls toString() which bypasses the visitor. We visit the base expression
     * through the visitor and redact JSON key identifiers that are string literals.
     */
    @Override
    public void visit(JsonExpression jsonExpression) {
      if (jsonExpression.getExpression() != null) {
        jsonExpression.getExpression().accept(this);
      }
      // Process each JSON path segment: operator + identifier
      // Operators like ->, ->>, #>, #>> are structural (preserved)
      // Identifiers that are string literals ('key') are redacted
      for (Map.Entry<String, String> entry : jsonExpression.getIdentList()) {
        String ident = entry.getKey();
        String operator = entry.getValue();
        if (operator != null) {
          getBuffer().append(operator);
        }
        if (ident != null) {
          appendRedactedKeyString(ident);
        }
      }
    }

    // =========================================================================
    // Search and match overrides — MATCH...AGAINST bypasses the visitor pattern
    // by using append(Object) on the against value instead of accept().
    // =========================================================================

    /**
     * MySQL MATCH(col1, col2) AGAINST ('search term' [modifier]).
     * Base class appends the against value via toString() bypass.
     * We visit the against value through the visitor to redact PII search terms.
     */
    @Override
    public void visit(FullTextSearch fullTextSearch) {
      getBuffer().append("MATCH (");
      // Match columns are Column references — structural, not PII
      if (fullTextSearch.getMatchColumns() != null) {
        int counter = 0;
        for (Column col : fullTextSearch.getMatchColumns()) {
          if (counter > 0) {
            getBuffer().append(", ");
          }
          getBuffer().append(col.getFullyQualifiedName());
          counter++;
        }
      }
      getBuffer().append(") AGAINST (");
      // The against value is user data — visit through the visitor
      if (fullTextSearch.getAgainstValue() != null) {
        fullTextSearch.getAgainstValue().accept(this);
      }
      if (fullTextSearch.getSearchModifier() != null) {
        getBuffer().append(" ");
        getBuffer().append(fullTextSearch.getSearchModifier());
      }
      getBuffer().append(")");
    }

    // =========================================================================
    // Relational operator overrides — IsDistinctExpression uses
    // append(Object) on left/right expressions, bypassing the visitor.
    // =========================================================================

    /**
     * {@code expr IS [NOT] DISTINCT FROM expr}.
     * Base class serializes left/right via append(Object) = toString() bypass.
     * We visit both sides through the visitor for proper redaction.
     */
    @Override
    public void visit(IsDistinctExpression isDistinctExpression) {
      isDistinctExpression.getLeftExpression().accept(this);
      getBuffer().append(isDistinctExpression.getStringExpression());
      isDistinctExpression.getRightExpression().accept(this);
    }

    // =========================================================================
    // MySQL-specific overrides — MySQLGroupConcat uses toString() bypass,
    // which serializes all expressions and separator without visitor.
    // =========================================================================

    /**
     * MySQL {@code GROUP_CONCAT([DISTINCT] expr [ORDER BY ...] [SEPARATOR 'sep'])}.
     * Base class calls toString() which bypasses the visitor entirely.
     * We traverse all sub-expressions through the visitor and redact the separator.
     */
    @Override
    public void visit(MySQLGroupConcat groupConcat) {
      getBuffer().append("GROUP_CONCAT(");
      if (groupConcat.isDistinct()) {
        getBuffer().append("DISTINCT ");
      }
      if (groupConcat.getExpressionList() != null) {
        visitExpressionList(groupConcat.getExpressionList());
      }
      if (groupConcat.getOrderByElements() != null
          && !groupConcat.getOrderByElements().isEmpty()) {
        getBuffer().append(" ORDER BY ");
        visitOrderByElements(groupConcat.getOrderByElements());
      }
      if (groupConcat.getSeparator() != null) {
        getBuffer().append(" SEPARATOR ");
        getBuffer().append(REDACTED_STRING);
      }
      getBuffer().append(")");
    }

    // =========================================================================
    // Oracle-specific overrides — these use toString() bypass which serializes
    // all sub-expressions without going through the visitor pattern.
    // =========================================================================

    /**
     * Oracle {@code CONNECT BY [NOCYCLE] expr START WITH expr} or
     * {@code START WITH expr CONNECT BY [NOCYCLE] expr}.
     * Base class calls toString() which bypasses the visitor.
     * We visit start/connect expressions through the visitor for redaction.
     */
    @Override
    public void visit(OracleHierarchicalExpression ohe) {
      if (ohe.isConnectFirst()) {
        getBuffer().append(" CONNECT BY ");
        if (ohe.isNoCycle()) {
          getBuffer().append("NOCYCLE ");
        }
        if (ohe.getConnectExpression() != null) {
          ohe.getConnectExpression().accept(this);
        }
        if (ohe.getStartExpression() != null) {
          getBuffer().append(" START WITH ");
          ohe.getStartExpression().accept(this);
        }
      } else {
        if (ohe.getStartExpression() != null) {
          getBuffer().append(" START WITH ");
          ohe.getStartExpression().accept(this);
        }
        getBuffer().append(" CONNECT BY ");
        if (ohe.isNoCycle()) {
          getBuffer().append("NOCYCLE ");
        }
        if (ohe.getConnectExpression() != null) {
          ohe.getConnectExpression().accept(this);
        }
      }
    }

    /**
     * Oracle {@code KEEP (DENSE_RANK FIRST/LAST ORDER BY expr)}.
     * Base class calls toString() which bypasses the visitor.
     * We visit the ORDER BY expressions through the visitor for redaction.
     */
    @Override
    public void visit(KeepExpression keepExpression) {
      getBuffer().append(" KEEP (");
      getBuffer().append(keepExpression.getName());
      getBuffer().append(keepExpression.isFirst() ? " FIRST" : " LAST");
      if (keepExpression.getOrderByElements() != null
          && !keepExpression.getOrderByElements().isEmpty()) {
        getBuffer().append(" ORDER BY ");
        visitOrderByElements(keepExpression.getOrderByElements());
      }
      getBuffer().append(")");
    }

    // =========================================================================
    // Condition overrides — OverlapsCondition uses toString() bypass which
    // serializes all sub-expressions without the visitor pattern.
    // =========================================================================

    /**
     * SQL {@code (expr1, expr2) OVERLAPS (expr3, expr4)}.
     * Base class calls toString() which bypasses the visitor.
     * We visit the left/right expression lists through the visitor for redaction.
     */
    @Override
    public void visit(OverlapsCondition overlapsCondition) {
      getBuffer().append("(");
      if (overlapsCondition.getLeft() != null) {
        visitExpressionList(overlapsCondition.getLeft());
      }
      getBuffer().append(") OVERLAPS (");
      if (overlapsCondition.getRight() != null) {
        visitExpressionList(overlapsCondition.getRight());
      }
      getBuffer().append(")");
    }


    // =========================================================================
    // Function override — the base class appends the KEEP expression via
    // toString() (append(Object)), bypassing the visitor pattern. We override
    // to route the KEEP expression through accept() for proper redaction.
    // =========================================================================

    /**
     * SQL function call: {@code name([DISTINCT|ALL|UNIQUE] params [ORDER BY ...])}
     * with optional {@code .attribute} and {@code KEEP (DENSE_RANK ...)}.
     * Base class bypasses the visitor forEEP expression via toString().
     * We override to call keep.accept(this) instead.
     */
    @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity"})
    @Override
    public void visit(Function function) {
      if (function.isEscaped()) {
        getBuffer().append("{fn ");
      }
      getBuffer().append(function.getName());
      if (function.getParameters() == null
          && function.getNamedParameters() == null) {
        getBuffer().append("()");
      } else {
        getBuffer().append("(");
        if (function.isDistinct()) {
          getBuffer().append("DISTINCT ");
        } else if (function.isAllColumns()) {
          getBuffer().append("ALL ");
        } else if (function.isUnique()) {
          getBuffer().append("UNIQUE ");
        }
        if (function.getNamedParameters() != null) {
          function.getNamedParameters().accept(this);
        }
        if (function.getParameters() != null) {
          function.getParameters().accept(this);
        }
        if (function.getOrderByElements() != null
            && !function.getOrderByElements().isEmpty()) {
          getBuffer().append(" ORDER BY ");
          visitOrderByElements(function.getOrderByElements());
        }
        getBuffer().append(")");
      }
      if (function.getAttribute() != null) {
        getBuffer().append(".");
        // getAttribute() returns a Column or Expression — structural SQL element
        getBuffer().append(function.getAttribute());
      }
      if (function.getKeep() != null) {
        // KEY FIX: visit KEEP via accept() instead of toString() bypass
        function.getKeep().accept(this);
      }
      if (function.isEscaped()) {
        getBuffer().append("}");
      }
    }

    // =========================================================================
    // AnalyticExpression override — the base class has several bypass paths:
    //   1. funcOrderBy: uses stream().map(toString()).collect() bypassing visitor
    //   2. WindowDefinition: orderBy and partitionBy use toString() methods
    //   3. WindowElement: appended via toString() bypassing vir
    // We override to route ALL sub-expressions through the visitor pattern.
    // =========================================================================

    /**
     * Window/analytic function: {@code name(expr) OVER (PARTITION BY ... ORDER BY ...)}.
     * Also handles WITHIN GROUP, FILTER, KEEP, and various window frame specs.
     */
    @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity"})
    @Override
    public void visit(AnalyticExpression ae) {
      getBuffer().append(ae.getName());
      getBuffer().append("(");
      if (ae.isDistinct()) {
        getBuffer().append("DISTINCT ");
      } else if (ae.isUnique()) {
        getBuffer().append("UNIQUE ");
      }
      Expression expr = ae.getExpression();
      if (expr != null) {
        expr.accept(this);
        if (ae.getOffset() != null) {
          getBuffer().append(", ");
          ae.getOffset().accept(this);
        }
        if (ae.getDefaultValue() != null) {
          getBuffer().append(", ");
          ae.getDefaultValue().accept(this);
        }
      } else if (ae.isAllColumns()) {
        getBuffer().append("*");
      }
      if (ae.isIgnoreNulls()) {
        getBuffer().append(" IGNORE NULLS");
      }
      // FIX: funcOrderBy — visit via visitor instead of stream/toString
      if (ae.getFuncOrderBy() != null && !ae.getFuncOrderBy().isEmpty()) {
        getBuffer().append(" ORDER BY ");
        visitOrderByElements(ae.getFuncOrderBy());
      }
      getBuffer().append(")");
      // KEEP — already uses accept() in base class, but we ensure it here
      if (ae.getKeep() != null) {
        ae.getKeep().accept(this);
        getBuffer().append(" ");
      }
      // FILTER
      if (ae.getFilterExpression() != null) {
        getBuffer().append("FILTER (WHERE ");
        ae.getFilterExpression().accept(this);
        getBuffer().append(")");
      }
      // IGNORE NULLS outside
      if (ae.isIgnoreNullsOutside()) {
        getBuffer().append(" IGNORE NULLS");
      }
      // Analytic type handling
      AnalyticType type = ae.getType();
      if (type != null) {
        getBuffer().append(" ");
        switch (type) {
          case WITHIN_GROUP:
            getBuffer().append("WITHIN GROUP");
            break;
          case WITHIN_GROUP_OVER:
            visitWithinGroupOver(ae);
            break;
          case FILTER_ONLY:
            break;
          default:
            visitOverClause(ae);
            break;
        }
      }
    }

    /**
     * XML aggregate serialization: {@code XMLSERIALIZE(XMLAGG(XMLTEXT(expr)
     * [ORDER BY ...]) AS type)}.
     * Base class appends ORDER BY elements via toString() bypass.
     * We visit ORDER BY expressions through the visitor for redaction.
     */
    @Override
    public void visit(XMLSerializeExpr xmlSerializeExpr) {
      getBuffer().append("xmlserialize(xmlagg(xmltext(");
      if (xmlSerializeExpr.getExpression() != null) {
        xmlSerializeExpr.getExpression().accept(this);
      }
      getBuffer().append(")");
      if (xmlSerializeExpr.getOrderByElements() != null
          && !xmlSerializeExpr.getOrderByElements().isEmpty()) {
        getBuffer().append(" ORDER BY ");
        visitOrderByElements(xmlSerializeExpr.getOrderByElements());
      }
      getBuffer().append(") AS ");
      // DataType is structural (type name like CLOB, VARCHAR) — not PII
      if (xmlSerializeExpr.getDataType() != null) {
        getBuffer().append(xmlSerializeExpr.getDataType());
      }
      getBuffer().append(")");
    }

    /**
     * Overrides the base utility method to handle the Oracle PRIOR keyword
     * in CONNECT BY expressions. The base ExpressionDeParser does not output
     * the PRIOR prefix, which is stored in the oraclePriorPosition property
     * of OldOracleJoinBinaryExpression (e.g., EqualsTo, GreaterThan).
     * <p>Position 1: PRIOR before left expression (e.g., PRIOR emp_id = mgr_id)</p>
     * <p>Position 2: PRIOR before right expression (e.g., emp_id = PRIOR mgr_id)</p>
     */
    @Override
    public void visitOldOracleJoinBinaryExpression(
        OldOracleJoinBinaryExpression expression, String operator) {
      if (expression.getOraclePriorPosition() == 1) {
        getBuffer().append("PRIOR ");
      }
      expression.getLeftExpression().accept(this);
      if (expression.getOldOracleJoinSyntax() == 1) {
        getBuffer().append("(+)");
      }
      getBuffer().append(operator);
      if (expression.getOraclePriorPosition() == 2) {
        getBuffer().append("PRIOR ");
      }
      expression.getRightExpression().accept(this);
      if (expression.getOldOracleJoinSyntax() == 2) {
        getBuffer().append("(+)");
      }
    }

    // =========================================================================
    // NON-EXPRESSION HELPER CLASSES — Security Audit Documentation
    //
    // The following classes from net.sf.jsqlparser.expression are NOT
    // Expression types (they do not implement the Expression interface and
    // are not part of the ExpressionVisitor). They are helper/support classes
    // used by Expression types or by the statement/select deparsers. Each has
    // been security-audited to confirm they do NOT contain user PII data that
    // needs redaction:
    //
    // - Alias (Serializable): Column/table alias names (e.g., "AS alias").
    //   Structural SQL metadata — not PII. Used by SelectDeParser for output.
    //
    // - AnyType (Enum): ANY, SOME, ALL keywords used by
    //   AnyComparisonExpression. Purely structural SQL keyword enum.
    //
    // - BinaryExpression (Abstract class): Base class for all binary operator
    //   expressions (Addition, EqualsTo, GreaterThan, etc.). Not directly
    //   visitable — concrete subclasses have their own visit() methods in
    //   ExpressionVisitor. The base visitBinaryExpression() in
    //   ExpressionDeParser properly uses accept() for left/right expressions.
    //
    // - ExpressionVisitor (Interface): The visitor interface we implement
    //   through ExpressionDeParser. Not a data-bearing class.
    //
    // - ExpressionVisitorAdapter (Class): A default implementation of
    //   ExpressionVisitor. We extend ExpressionDeParser instead, which
    //   provides richer deparser behavior. Not used directly.
    //
    // - FilterOverImpl (Class): Helper for FILTER/OVER clauses containing
    //   partitionExpressionList, orderByElements, filterExpression, and
    //   windowElement. Its append() method uses toString() bypass. This is
    //   already handled by our explicit AnalyticExpression override, which
    //   traverses all these sub-components through the visitor pattern via
    //   visitOverClause(), visitExpressionList(), visitOrderByElements(),
    //   and visitWindowElement() helpers.
    //
    // - MySQLIndexHint (Serializable): MySQL table index hints like
    //   USE INDEX (idx1), FORCE INDEX (idx2), IGNORE INDEX (idx3).
    //   Contains only structural metadata (action, indexQualifier, indexNames).
    //   Index names are schema metadata, not PII. Rendered by SelectDeParser
    //   during FROM clause processing.
    //
    // - OrderByClause (Serializable): Helper for ORDER BY elements. Its
    //   toStringOrderByElements() method uses toString() bypass. This is
    //   already handled by our visitOrderByElements() helper method, which
    //   visits each ORDER BY expression through the visitor pattern.
    //
    // - PartitionByClause (Serializable): Helper for PARTITION BY elements.
    //   Its toStringPartitionBy() method uses toString() bypass. This is
    //   already handled by our visitExpressionList() helper method, which
    //   visits each partition expression through the visitor pattern.
    //
    // - SQLServerHints (Serializable): SQL Server table hints like
    //   WITH (NOLOCK), WITH (INDEX = idx1). Contains only structural metadata
    //   (noLock flag, indexName). These are optimizer directives, not PII.
    //   Rendered by SelectDeParser during FROM clause processing.
    // =========================================================================

    // =========================================================================
    // Helper methods — placed after ALL visit() overrides to satisfy the
    // Checkstyle rule: "All overloaded methods should be placed next to
    // each other."
    // =========================================================================

    /**
     * Handles standard SQL JSON_OBJECT with KEY/VALUE syntax:
     * JSON_OBJECT([KEY] key VALUE value [FORMAT JSON], ... [NULL ON NULL]
     * [WITH UNIQUE KEYS]) or colon syntax: JSON_OBJECT(key: value, ...)
     */
    private void visitJsonObject(JsonFunction jsonFunction) {
      getBuffer().append("JSON_OBJECT(");
      int counter = 0;
      for (JsonKeyValuePair kvp : jsonFunction.getKeyValuePairs()) {
        if (counter > 0) {
          getBuffer().append(", ");
        }
        if (kvp.isUsingValueKeyword()) {
          if (kvp.isUsingKeyKeyword()) {
            getBuffer().append("KEY ");
          }
          appendRedactedKeyString(kvp.getKey());
          getBuffer().append(" VALUE ");
          visitJsonValue(kvp.getValue());
        } else {
          appendRedactedKeyString(kvp.getKey());
          getBuffer().append(": ");
          visitJsonValue(kvp.getValue());
        }
        if (kvp.isUsingFormatJson()) {
          getBuffer().append(" FORMAT JSON");
        }
        counter++;
      }
      appendOnNullType(jsonFunction.getOnNullType());
      appendUniqueKeysType(jsonFunction.getUniqueKeysType());
      getBuffer().append(")");
    }

    /**
     * Handles PostgreSQL json_build_object / JSON_OBJECT syntax:
     * JSON_OBJECT(key[, value] key[, value] ...)
     */
    private void visitJsonPostgresObject(JsonFunction jsonFunction) {
      getBuffer().append("JSON_OBJECT(");
      for (JsonKeyValuePair kvp : jsonFunction.getKeyValuePairs()) {
        appendRedactedKeyString(kvp.getKey());
        if (kvp.getValue() != null) {
          getBuffer().append(", ");
          visitJsonValue(kvp.getValue());
        }
      }
      getBuffer().append(")");
    }

    /**
     * Handles MySQL JSON_OBJECT syntax:
     * JSON_OBJECT(key, value, key, value, ...)
     */
    private void visitJsonMySqlObject(JsonFunction jsonFunction) {
      getBuffer().append("JSON_OBJECT(");
      int counter = 0;
      for (JsonKeyValuePair kvp : jsonFunction.getKeyValuePairs()) {
        if (counter > 0) {
          getBuffer().append(", ");
        }
        appendRedactedKeyString(kvp.getKey());
        getBuffer().append(", ");
        visitJsonValue(kvp.getValue());
        counter++;
      }
      getBuffer().append(")");
    }

    /**
     * Handles JSON_ARRAY syntax:
     * JSON_ARRAY(expr [FORMAT JSON], expr [FORMAT JSON], ...
     *            [NULL ON NULL] [WITH UNIQUE KEYS])
     */
    private void visitJsonArray(JsonFunction jsonFunction) {
      getBuffer().append("JSON_ARRAY(");
      int counter = 0;
      for (JsonFunctionExpression expr : jsonFunction.getExpressions()) {
        if (counter > 0) {
          getBuffer().append(", ");
        }
        expr.getExpression().accept(this);
        if (expr.isUsingFormatJson()) {
          getBuffer().append(" FORMAT JSON");
        }
        counter++;
      }
      appendOnNullType(jsonFunction.getOnNullType());
      appendUniqueKeysType(jsonFunction.getUniqueKeysType());
      getBuffer().append(")");
    }

    /**
     * Handles JSON_OBJECTAGG:
     * JSON_OBJECTAGG([KEY] key VALUE value [FORMAT JSON]
     *   [NULL ON NULL] [WITH UNIQUE KEYS])
     */
    private void visitJsonObjectAgg(JsonAggregateFunction jsonAgg) {
      getBuffer().append("JSON_OBJECTAGG(");
      if (jsonAgg.isUsingValueKeyword()) {
        if (jsonAgg.isUsingKeyKeyword()) {
          getBuffer().append("KEY ");
        }
        appendRedactedKeyString(jsonAgg.getKey());
        getBuffer().append(" VALUE ");
        visitJsonValue(jsonAgg.getValue());
      } else {
        appendRedactedKeyString(jsonAgg.getKey());
        getBuffer().append(": ");
        visitJsonValue(jsonAgg.getValue());
      }
      if (jsonAgg.isUsingFormatJson()) {
        getBuffer().append(" FORMAT JSON");
      }
      appendOnNullType(jsonAgg.getOnNullType());
      appendUniqueKeysType(jsonAgg.getUniqueKeysType());
      getBuffer().append(")");
    }

    /**
     * Handles JSON_ARRAYAGG:
     * JSON_ARRAYAGG(expr [FORMAT JSON] [ORDER BY ...] [NULL ON NULL])
     */
    private void visitJsonArrayAgg(JsonAggregateFunction jsonAgg) {
      getBuffer().append("JSON_ARRAYAGG(");
      if (jsonAgg.getExpression() != null) {
        jsonAgg.getExpression().accept(this);
      }
      if (jsonAgg.isUsingFormatJson()) {
        getBuffer().append(" FORMAT JSON");
      }
      appendOnNullType(jsonAgg.getOnNullType());
      getBuffer().append(")");
    }

    /**
     * Redacts a JSON key string if it is a string literal (starts and ends with
     * single quotes). Column references and other identifier-style keys are
     * preserved as they represent query structure, not user data.
     */
    private void appendRedactedKeyString(String key) {
      if (key == null) {
        return;
      }
      String trimmedKey = key.trim();
      if (trimmedKey.length() >= 2
          && trimmedKey.charAt(0) == '\''
          && trimmedKey.charAt(trimmedKey.length() - 1) == '\'') {
        getBuffer().append(REDACTED_STRING);
      } else {
        getBuffer().append(key);
      }
    }

    /**
     * Visits a JSON value through the visitor pattern if it is an Expression.
     * For non-Expression values (rare edge case), applies string redaction.
     */
    private void visitJsonValue(Object value) {
      if (value instanceof Expression) {
        ((Expression) value).accept(this);
      } else if (value != null) {
        getBuffer().append(REDACTED_STRING);
      }
    }

    /**
     * Appends the ON NULL clause if present.
     */
    private void appendOnNullType(JsonAggregateOnNullType onNullType) {
      if (onNullType != null) {
        switch (onNullType) {
          case NULL:
            getBuffer().append(" NULL ON NULL");
            break;
          case ABSENT:
            getBuffer().append(" ABSENT ON NULL");
            break;
          default:
            break;
        }
      }
    }

    /**
     * Appends the UNIQUE KEYS clause if present.
     */
    private void appendUniqueKeysType(JsonAggregateUniqueKeysType uniqueKeysType) {
      if (uniqueKeysType != null) {
        switch (uniqueKeysType) {
          case WITH:
            getBuffer().append(" WITH UNIQUE KEYS");
            break;
          case WITHOUT:
            getBuffer().append(" WITHOUT UNIQUE KEYS");
            break;
          default:
            break;
        }
      }
    }

    /**
     * Visits each expression in an ExpressionList through the visitor pattern,
     * outputting them comma-separated. Used by MySQLGroupConcat, OverlapsCondition.
     */
    private void visitExpressionList(ExpressionList<?> exprList) {
      int counter = 0;
      for (Expression expr : exprList) {
        if (counter > 0) {
          getBuffer().append(", ");
        }
        expr.accept(this);
        counter++;
      }
    }

    /**
     * Handles the WITHIN GROUP (ORDER BY ...) OVER (PARTITION BY ...) clause
     * of an AnalyticExpression. This path bypasses the visitor in the base
     * class via WindowDefinition.toStringOrderByElements() and
     * toStringPartitionBy(). We visit all expressions through the visitor.
     */
    private void visitWithinGroupOver(AnalyticExpression ae) {
      getBuffer().append("WITHIN GROUP (");
      WindowDefinition wd = ae.getWindowDefinition();
      if (wd != null && wd.getOrderByElements() != null
          && !wd.getOrderByElements().isEmpty()) {
        getBuffer().append("ORDER BY ");
        visitOrderByElements(wd.getOrderByElements());
      }
      getBuffer().append(") OVER (");
      if (wd != null && wd.getPartitionExpressionList() != null
          && !wd.getPartitionExpressionList().isEmpty()) {
        getBuffer().append("PARTITION BY ");
        visitExpressionList(wd.getPartitionExpressionList());
        getBuffer().append(" ");
      }
      getBuffer().append(")");
    }

    /**
     * Handles the OVER clause of an AnalyticExpression. For named windows
     * (OVER window_name), outputs just the name. For inline windows, visits
     * PARTITION BY, ORDER BY, and window frame specifications through the
     * visitor pattern.
     */
    @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity"})
    private void visitOverClause(AnalyticExpression ae) {
      getBuffer().append("OVER");
      String windowName = ae.getWindowName();
      if (windowName != null && !windowName.isEmpty()) {
        getBuffer().append(" ");
        getBuffer().append(windowName);
      } else {
        getBuffer().append(" (");
        ExpressionList<?> partExprs = ae.getPartitionExpressionList();
        if (partExprs != null && !partExprs.isEmpty()) {
          if (ae.isPartitionByBrackets()) {
            getBuffer().append("(");
          }
          getBuffer().append("PARTITION BY ");
          visitExpressionList(partExprs);
          if (ae.isPartitionByBrackets()) {
            getBuffer().append(")");
          }
          getBuffer().append(" ");
        }
        List<OrderByElement> orderBy = ae.getOrderByElements();
        if (orderBy != null && !orderBy.isEmpty()) {
          getBuffer().append("ORDER BY ");
          visitOrderByElements(orderBy);
        }
        // FIX: WindowElement — visit expressions instead of toString()
        WindowElement we = ae.getWindowElement();
        if (we != null) {
          if (orderBy != null && !orderBy.isEmpty()) {
            getBuffer().append(" ");
          }
          visitWindowElement(we);
        }
        getBuffer().append(")");
      }
    }

    /**
     * Visits a WindowElement (ROWS/RANGE frame spec) through the visitor pattern
     * instead of using toString(). This ensures numeric frame offsets like
     * {@code ROWS 5 PRECEDING} are properly redacted.
     */
    private void visitWindowElement(WindowElement we) {
      getBuffer().append(we.getType()); // ROWS or RANGE
      if (we.getOffset() != null) {
        visitWindowOffset(we.getOffset());
      } else if (we.getRange() != null) {
        visitWindowRange(we.getRange());
      }
    }

    /**
     * Visits a WindowOffset expression through the visitor pattern.
     * Handles formats like: {@code 5 PRECEDING}, {@code UNBOUNDED FOLLOWING},
     * {@code CURRENT ROW}.
     */
    private void visitWindowOffset(WindowOffset wo) {
      if (wo.getExpression() != null) {
        getBuffer().append(" ");
        wo.getExpression().accept(this);
        if (wo.getType() != null) {
          getBuffer().append(" ");
          getBuffer().append(wo.getType());
        }
      } else if (wo.getType() != null) {
        switch (wo.getType()) {
          case PRECEDING:
            getBuffer().append(" UNBOUNDED PRECEDING");
            break;
          case FOLLOWING:
            getBuffer().append(" UNBOUNDED FOLLOWING");
            break;
          case CURRENT:
            getBuffer().append(" CURRENT ROW");
            break;
          default:
            break;
        }
      }
    }

    /**
     * Visits a WindowRange (BETWEEN ... AND ...) through the visitor pattern.
     */
    private void visitWindowRange(WindowRange wr) {
      getBuffer().append(" BETWEEN");
      if (wr.getStart() != null) {
        visitWindowOffset(wr.getStart());
      }
      getBuffer().append(" AND");
      if (wr.getEnd() != null) {
        visitWindowOffset(wr.getEnd());
      }
    }

    /**
     * Visits each ORDER BY element through the visitor pattern, preserving the
     * ASC/DESC and NULLS FIRST/LAST modifiers while redacting any literal values
     * in the ORDER BY expressions. Used by MySQLGroupConcat, KeepExpression.
     */
    private void visitOrderByElements(List<OrderByElement> orderByElements) {
      int counter = 0;
      for (OrderByElement orderBy : orderByElements) {
        if (counter > 0) {
          getBuffer().append(", ");
        }
        orderBy.getExpression().accept(this);
        if (orderBy.isAscDescPresent()) {
          getBuffer().append(orderBy.isAsc() ? " ASC" : " DESC");
        }
        if (orderBy.getNullOrdering() != null) {
          getBuffer().append(" NULLS ");
          getBuffer().append(orderBy.getNullOrdering().name());
        }
        counter++;
      }
    }
  }

  public static void validateSqlSyntax(String sql) throws JSQLParserException {
    if (sql == null || sql.trim().isEmpty()) {
      return;
    }
    CCJSqlParserUtil.parse(sql);
  }
}
