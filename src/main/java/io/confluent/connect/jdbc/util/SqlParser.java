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
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnalyticType;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
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
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OverlapsCondition;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WindowDefinition;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.expression.WindowOffset;
import net.sf.jsqlparser.expression.WindowRange;
import net.sf.jsqlparser.expression.XMLSerializeExpr;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.FullTextSearch;
import net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
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

  // Matches single-quoted SQL string literals (handles '' and \' escaping)
  static final Pattern STRING_LITERAL_PATTERN =
      Pattern.compile("'(?:[^'\\\\]|\\\\.|'')*'");

  // Matches PostgreSQL dollar-quoted strings: $$...$$ or $tag$...$tag$
  static final Pattern DOLLAR_QUOTED_PATTERN =
      Pattern.compile("\\$(\\w*)\\$.*?\\$\\1\\$", Pattern.DOTALL);

  // Matches standalone numeric literals, excluding numbers within identifiers
  static final Pattern NUMERIC_LITERAL_PATTERN =
      Pattern.compile("(?<![a-zA-Z_$\\d])\\d+(?:\\.\\d+)?(?![a-zA-Z_$\\d])");

  // Structural number positions (TOP n, OPTIMIZE FOR n) — SQL syntax, not PII
  static final Pattern[] STRUCTURAL_NUMBER_PATTERNS = {
      Pattern.compile("(?i)\\bTOP\\s+(\\d+(?:\\.\\d+)?)"),
      Pattern.compile("(?i)\\bOPTIMIZE\\s+FOR\\s+(\\d+(?:\\.\\d+)?)"),
  };

  // Type definition parameters like DECIMAL(10, 2), VARCHAR(255) — metadata, not PII
  static final Pattern TYPE_DEF_PARAMS_PATTERN = Pattern.compile(
      "(?i)\\b(?:DECIMAL|NUMERIC|VARCHAR|NVARCHAR|CHAR|NCHAR|FLOAT|REAL|"
      + "NUMBER|BINARY|VARBINARY|BIT|DATETIME2|DATETIMEOFFSET|TIME|TIMESTAMP|"
      + "RAW|CLOB|BLOB|NCLOB)\\s*\\(([^)]+)\\)"
  );

  private static final Pattern INNER_NUMBER_PATTERN =
      Pattern.compile("\\d+(?:\\.\\d+)?");

  /**
   * Redacts PII from a SQL query while preserving structure using a multi-layered approach:
   * Layer 1 (AST visitor), Layer 2 (regex safety net for leaked strings),
   * Layer 3 (post-redaction verification), Layer 4 (regex fallback when parsing fails).
   */
  public static String redactSensitiveData(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return "";
    }

    try {
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

      // Layer 2: Regex safety net for string literals that leaked through bypass types
      result = redactStringLiterals(result);

      // Layer 3: Verify no unredacted literals remain
      if (hasLeakedLiterals(result)) {
        log.warn("Detected potential unredacted literals after AST processing, "
            + "fully redacting query for safety");
        return REDACTED_VALUE;
      }

      return result;
    } catch (JSQLParserException e) {
      // Layer 4: Regex fallback when AST parsing fails
      log.debug("JSqlParser could not parse the SQL query, "
          + "falling back to regex redaction", e);
      return redactAllLiterals(sql);
    }
  }

  /** Redacts dollar-quoted and single-quoted string literals using regex. */
  static String redactStringLiterals(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    String result = DOLLAR_QUOTED_PATTERN.matcher(sql).replaceAll(REDACTED_STRING);
    return STRING_LITERAL_PATTERN.matcher(result).replaceAll(REDACTED_STRING);
  }

  /** Redacts all literals (strings and numbers) using regex. Fallback when AST fails. */
  static String redactAllLiterals(String sql) {
    if (sql == null || sql.isEmpty()) {
      return REDACTED_VALUE;
    }
    try {
      String result = DOLLAR_QUOTED_PATTERN.matcher(sql).replaceAll(REDACTED_STRING);
      result = STRING_LITERAL_PATTERN.matcher(result).replaceAll(REDACTED_STRING);
      result = NUMERIC_LITERAL_PATTERN.matcher(result).replaceAll(REDACTED_NUMBER);
      return result;
    } catch (Exception e) {
      return REDACTED_VALUE;
    }
  }

  /**
   * Checks if any unredacted literals remain after AST and regex processing.
   * Verifies dollar-quoted strings, single-quoted strings, and standalone numbers.
   */
  static boolean hasLeakedLiterals(String redactedSql) {
    if (redactedSql == null || redactedSql.isEmpty()) {
      return false;
    }

    if (DOLLAR_QUOTED_PATTERN.matcher(redactedSql).find()) {
      return true;
    }

    Matcher stringMatcher = STRING_LITERAL_PATTERN.matcher(redactedSql);
    while (stringMatcher.find()) {
      if (!REDACTED_STRING.equals(stringMatcher.group())) {
        return true;
      }
    }

    Set<Integer> structuralPositions = findStructuralNumberPositions(redactedSql);
    Matcher numMatcher = NUMERIC_LITERAL_PATTERN.matcher(redactedSql);
    while (numMatcher.find()) {
      String numStr = numMatcher.group();
      try {
        if (Double.parseDouble(numStr) == 0.0) {
          continue;
        }
      } catch (NumberFormatException e) {
        // Not a valid number; treat as potential leak
      }
      if (structuralPositions.contains(numMatcher.start())) {
        continue;
      }
      return true;
    }

    return false;
  }

  /** Finds positions of structural numbers (TOP n, OPTIMIZE FOR n, type params). */
  private static Set<Integer> findStructuralNumberPositions(String sql) {
    Set<Integer> positions = new HashSet<>();

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
   * Redacting expression visitor that replaces literal values with redaction markers.
   * Overrides only expression types that need intervention: leaf literals and types
   * where the base {@link ExpressionDeParser} uses toString() bypassing the visitor
   * pattern. All other expression types are handled safely by the base class via accept().
   */
  private static class RedactingExpressionDeParser extends ExpressionDeParser {

    // --- Leaf literal overrides ---

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
      getBuffer().append("NULL");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
      getBuffer().append(REDACTED_STRING);
    }

    // --- Compound expression overrides ---

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

    // Base class uses toString() for left expression — override to use accept()
    @Override
    public void visit(CollateExpression collateExpression) {
      collateExpression.getLeftExpression().accept(this);
      getBuffer().append(" COLLATE ");
      getBuffer().append(collateExpression.getCollate());
    }

    // Base class parameter path uses toString() bypass — redact directly
    @Override
    public void visit(IntervalExpression intervalExpression) {
      if (intervalExpression.isUsingIntervalKeyword()) {
        getBuffer().append("INTERVAL ");
      }
      if (intervalExpression.getExpression() != null) {
        intervalExpression.getExpression().accept(this);
      } else {
        getBuffer().append(REDACTED_NUMBER);
      }
      if (intervalExpression.getIntervalType() != null) {
        getBuffer().append(" ");
        getBuffer().append(intervalExpression.getIntervalType());
      }
    }

    // Base class uses getName() — override to preserve exact format
    @Override
    public void visit(NextValExpression nextValExpression) {
      getBuffer().append(
          nextValExpression.isUsingNextValueFor() ? "NEXT VALUE FOR " : "NEXTVAL FOR "
      );
      getBuffer().append(nextValExpression.getName());
    }

    // --- toString()-bypass overrides (custom traversal required for PII safety) ---

    // Base class calls append(buffer)/toString() on JSON key-value pairs
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

    // Base class calls toString() — override to visit base expression and redact keys
    @Override
    public void visit(JsonExpression jsonExpression) {
      if (jsonExpression.getExpression() != null) {
        jsonExpression.getExpression().accept(this);
      }
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

    // Base class appends against value via toString() bypass
    @Override
    public void visit(FullTextSearch fullTextSearch) {
      getBuffer().append("MATCH (");
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
      if (fullTextSearch.getAgainstValue() != null) {
        fullTextSearch.getAgainstValue().accept(this);
      }
      if (fullTextSearch.getSearchModifier() != null) {
        getBuffer().append(" ");
        getBuffer().append(fullTextSearch.getSearchModifier());
      }
      getBuffer().append(")");
    }

    // Base class serializes left/right via append(Object) = toString() bypass
    @Override
    public void visit(IsDistinctExpression isDistinctExpression) {
      isDistinctExpression.getLeftExpression().accept(this);
      getBuffer().append(isDistinctExpression.getStringExpression());
      isDistinctExpression.getRightExpression().accept(this);
    }

    // Base class calls toString() bypassing visitor entirely
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

    // Base class calls toString() bypassing visitor
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

    // Base class calls toString() on ORDER BY elements
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

    // Base class calls toString() bypassing visitor
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

    // Base class appends KEEP expression via toString() — override to use accept()
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
        getBuffer().append(function.getAttribute());
      }
      if (function.getKeep() != null) {
        function.getKeep().accept(this);
      }
      if (function.isEscaped()) {
        getBuffer().append("}");
      }
    }

    // Base class bypasses visitor for funcOrderBy, WindowDefinition, WindowElement
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
      if (ae.getFuncOrderBy() != null && !ae.getFuncOrderBy().isEmpty()) {
        getBuffer().append(" ORDER BY ");
        visitOrderByElements(ae.getFuncOrderBy());
      }
      getBuffer().append(")");
      if (ae.getKeep() != null) {
        ae.getKeep().accept(this);
        getBuffer().append(" ");
      }
      if (ae.getFilterExpression() != null) {
        getBuffer().append("FILTER (WHERE ");
        ae.getFilterExpression().accept(this);
        getBuffer().append(")");
      }
      if (ae.isIgnoreNullsOutside()) {
        getBuffer().append(" IGNORE NULLS");
      }
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

    // Base class appends ORDER BY elements via toString() bypass
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
      if (xmlSerializeExpr.getDataType() != null) {
        getBuffer().append(xmlSerializeExpr.getDataType());
      }
      getBuffer().append(")");
    }

    // Handles Oracle PRIOR keyword which the base class omits
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

    // --- Helper methods ---

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

    /** Redacts string literal keys while preserving column reference keys. */
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

    private void visitJsonValue(Object value) {
      if (value instanceof Expression) {
        ((Expression) value).accept(this);
      } else if (value != null) {
        getBuffer().append(REDACTED_STRING);
      }
    }

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

    private void visitWindowElement(WindowElement we) {
      getBuffer().append(we.getType());
      if (we.getOffset() != null) {
        visitWindowOffset(we.getOffset());
      } else if (we.getRange() != null) {
        visitWindowRange(we.getRange());
      }
    }

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
