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
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

public class SqlParser {

  private static final String REDACTED_STRING = "'********'";
  private static final String REDACTED_NUMBER = "0";
  private static final String REDACTED_VALUE = "<redacted>";

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
      return buffer.toString();
    } catch (JSQLParserException e) {
      return REDACTED_VALUE;
    }
  }

  private static class RedactingExpressionDeParser extends ExpressionDeParser {
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
    public void visit(JdbcParameter jdbcParameter) {
      getBuffer().append(REDACTED_STRING);
    }

    @Override
    public void visit(ArrayExpression arrayExpression) {
      getBuffer().append(REDACTED_STRING);
    }

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
  }
}
