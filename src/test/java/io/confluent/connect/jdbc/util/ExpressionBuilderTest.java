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

package io.confluent.connect.jdbc.util;

import java.util.Collections;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExpressionBuilderTest {

  protected static final TableId TABLE_ID_T1 = new TableId("c", "s", "t1");
  protected static final ColumnId COLUMN_ID_T1_A = new ColumnId(TABLE_ID_T1, "c1");

  protected static final TableId TABLE_ID_T2 = new TableId(null, null, "t2");
  protected static final ColumnId COLUMN_ID_T2_A = new ColumnId(TABLE_ID_T2, "c2");

  private IdentifierRules rules;
  private QuoteMethod quoteIdentifiers;

  @Before
  public void setup() {
    rules = null;
    quoteIdentifiers = QuoteMethod.ALWAYS;
  }

  @Test
  public void shouldQuoteTableNamesByDefault() {
    assertExpression("\"c\".\"s\".\"t1\"", b-> b.append(TABLE_ID_T1));
    assertExpression("\"t1\"", b-> b.appendTableName(TABLE_ID_T1.tableName()));

    assertExpression("\"t2\"", b-> b.append(TABLE_ID_T2));
    assertExpression("\"t2\"", b-> b.appendTableName(TABLE_ID_T2.tableName()));
  }

  @Test
  public void shouldQuoteTableNamesOnlyWhenSet() {
    quoteIdentifiers = QuoteMethod.ALWAYS;
    assertExpression("\"c\".\"s\".\"t1\"", b-> b.append(TABLE_ID_T1));
    assertExpression("\"t1\"", b-> b.appendTableName(TABLE_ID_T1.tableName()));

    assertExpression("\"t2\"", b-> b.append(TABLE_ID_T2));
    assertExpression("\"t2\"", b-> b.appendTableName(TABLE_ID_T2.tableName()));

    quoteIdentifiers = QuoteMethod.NEVER;
    assertExpression("c.s.t1", b-> b.append(TABLE_ID_T1));
    assertExpression("t1", b-> b.appendTableName(TABLE_ID_T1.tableName()));

    assertExpression("t2", b-> b.append(TABLE_ID_T2));
    assertExpression("t2", b-> b.appendTableName(TABLE_ID_T2.tableName()));
  }

  @Test
  public void shouldQuoteTableNamesAndColumnNamesByDefault() {
    assertExpression("\"c\".\"s\".\"t1\".\"c1\"", b-> b.append(COLUMN_ID_T1_A));
    assertExpression("\"t2\".\"c2\"", b-> b.append(COLUMN_ID_T2_A));
  }

  @Test
  public void shouldQuoteTableNamesAndColumnNamesOnlyWhenSet() {
    quoteIdentifiers = QuoteMethod.ALWAYS;
    assertExpression("\"c\".\"s\".\"t1\".\"c1\"", b-> b.append(COLUMN_ID_T1_A));
    assertExpression("\"t2\".\"c2\"", b-> b.append(COLUMN_ID_T2_A));

    quoteIdentifiers = QuoteMethod.NEVER;
    assertExpression("c.s.t1.c1", b-> b.append(COLUMN_ID_T1_A));
    assertExpression("t2.c2", b-> b.append(COLUMN_ID_T2_A));
  }

  @Test
  public void shouldQuoteColumnNamesWhenSet() {
    quoteIdentifiers = QuoteMethod.ALWAYS;
    assertExpression("\"c1\"", b-> b.appendColumnName(COLUMN_ID_T1_A.name()));
    assertExpression("\"c2\"", b-> b.appendColumnName(COLUMN_ID_T2_A.name()));

    quoteIdentifiers = QuoteMethod.NEVER;
    assertExpression("c1", b-> b.appendColumnName(COLUMN_ID_T1_A.name()));
    assertExpression("c2", b-> b.appendColumnName(COLUMN_ID_T2_A.name()));
  }

  @Test
  public void shouldEscapeEmbeddedQuoteCharacterInIdentifiers() {
    // An embedded closing-quote character is neutralized by doubling the dialect's closing quote,
    // so it cannot terminate the identifier early and append trailing SQL.

    // Default double-quote dialect.
    assertExpression("\"a\"\"b\"", b -> b.appendColumnName("a\"b"));

    // MySQL / SQLite backtick dialect.
    rules = new IdentifierRules(".", "`", "`");
    assertExpression("`a``b`", b -> b.appendColumnName("a`b"));

    // SQL Server bracket dialect: only the closing bracket is doubled; the opening bracket is
    // not special inside a bracketed identifier and so is left as-is.
    rules = new IdentifierRules(".", "[", "]");
    assertExpression("[a]]b]", b -> b.appendColumnName("a]b"));
    assertExpression("[a[b]", b -> b.appendColumnName("a[b"));
  }

  @Test
  public void shouldAcceptOrdinaryIdentifierWhenQuotingDisabled() {
    // Ordinary names (the shape Avro enforces: [A-Za-z_][A-Za-z0-9_]*) must still be accepted in
    // NEVER mode, so the hardening cannot break the overwhelming majority of real connectors.
    quoteIdentifiers = QuoteMethod.NEVER;
    assertExpression("my_col1", b -> b.appendColumnName("my_col1"));
  }

  @Test(expected = ConnectException.class)
  public void shouldRejectStatementBreakingIdentifierWhenQuotingDisabled() {
    // In NEVER mode the name is emitted with no surrounding quotes, so quote-doubling cannot help:
    // a name carrying a statement separator and comment introducer would start a second statement.
    // The only defense is to reject it outright. (Previously this was emitted verbatim.)
    quoteIdentifiers = QuoteMethod.NEVER;
    assertExpression("unused", b -> b.appendColumnName("x INT); DROP TABLE t; --"));
  }

  @Test
  public void shouldContainStatementBreakingIdentifierWhenQuoted() {
    // In quoted mode the closing quote is doubled, so the whole name stays one identifier token:
    // the separator, the comment introducer and the trailing text are all inside the quotes.
    // Quote-doubling alone is sufficient here; no sequence rejection is needed. (This used to be
    // rejected as well, which needlessly broke legitimately named fields in the default mode.)
    String name = "evil\"); DROP TABLE t; --";
    assertExpression("\"evil\"\"); DROP TABLE t; --\"", b -> b.appendColumnName(name));
    assertQuotedIdentifierClosesOnlyAtEnd(name);
  }

  @Test
  public void shouldEscapeEmbeddedQuoteInEveryIdentifierAppendMethod() {
    assertExpression("\"a\"\"b\"", b -> b.appendIdentifier("a\"b", QuoteMethod.ALWAYS));
    assertExpression("\"a\"\"b\"", b -> b.appendTableName("a\"b"));
    assertExpression("\"a\"\"b\"", b -> b.appendColumnName("a\"b"));
    assertExpression("\"a\"\"b\"", b -> b.appendIdentifierQuoted("a\"b"));
    assertExpression("\"c\"\"\".\"s\".\"t\"\"\"", b -> b.append(new TableId("c\"", "s", "t\"")));
    assertExpression("\"t\"\"\".\"c\"\"\"",
        b -> b.append(new ColumnId(new TableId(null, null, "t\""), "c\"")));
  }

  @Test
  public void shouldAcceptOrdinaryPunctuationInQuotedIdentifiers() {
    // Inside a quoted identifier spaces, dots, hyphens, parentheses and commas are inert, so
    // legitimate names using them keep working in the default ALWAYS mode.
    assertExpression("\"my col (x), y.z-w\"", b -> b.appendColumnName("my col (x), y.z-w"));
  }

  @Test
  public void shouldRejectControlCharsInAnyQuoteMode() {
    // A control character is never part of a legitimate name and can hide or split the statement
    // in logs and tooling, so it is rejected in both modes as defense in depth.
    String[] dangerous = {"a\nb", "a\rb", "a\tb", "a\u0000b", "a\u007fb"};
    for (QuoteMethod mode : QuoteMethod.values()) {
      quoteIdentifiers = mode;
      for (String name : dangerous) {
        assertRejected(name, b -> b.appendColumnName(name));
        assertRejected(name, b -> b.appendTableName(name));
        assertRejected(name, b -> b.appendIdentifier(name, mode));
      }
    }
    for (String name : dangerous) {
      assertRejected(name, b -> b.appendIdentifierQuoted(name));
    }
  }

  @Test
  public void shouldAcceptStatementSeparatorAndCommentsInQuotedIdentifiers() {
    // Inside a quoted identifier ';', '--', '/*' and '*/' are ordinary characters, so a field
    // legitimately named e.g. "order;id" or "a--b" keeps producing the same quoted identifier it
    // did before in the default ALWAYS mode, for every quote style.
    String[] names = {"a;b", "order;id", "a--b", "a/*b", "a*/b", "x); DROP TABLE t; --"};
    for (String name : names) {
      assertExpression("\"" + name + "\"", b -> b.appendColumnName(name));
      assertExpression("\"" + name + "\"", b -> b.appendTableName(name));
      assertExpression("\"" + name + "\"", b -> b.appendIdentifier(name, QuoteMethod.ALWAYS));
      assertExpression("\"" + name + "\"", b -> b.appendIdentifierQuoted(name));
      assertQuotedIdentifierClosesOnlyAtEnd(name);
    }
    rules = new IdentifierRules(".", "`", "`");
    assertExpression("`order;id`", b -> b.appendColumnName("order;id"));
    rules = new IdentifierRules(".", "[", "]");
    assertExpression("[a--b]", b -> b.appendColumnName("a--b"));
  }

  @Test
  public void shouldRejectStatementSeparatorAndCommentsInUnquotedIdentifiers() {
    // A bare name has nothing to contain these sequences: ';' ends the statement and the comment
    // introducers hide the rest of it, so they are rejected when the name is emitted unquoted.
    String[] dangerous = {"a;b", "order;id", "a--b", "a/*b", "a*/b"};
    quoteIdentifiers = QuoteMethod.NEVER;
    for (String name : dangerous) {
      assertRejected(name, b -> b.appendColumnName(name));
      assertRejected(name, b -> b.appendTableName(name));
      assertRejected(name, b -> b.appendIdentifier(name, QuoteMethod.NEVER));
    }
    quoteIdentifiers = QuoteMethod.ALWAYS;
    rules = new IdentifierRules(".", IdentifierRules.UNSUPPORTED_QUOTE,
        IdentifierRules.UNSUPPORTED_QUOTE);
    for (String name : dangerous) {
      assertRejected(name, b -> b.appendColumnName(name));
    }
  }

  @Test
  public void shouldRejectSyntaxCharactersInUnquotedIdentifiers() {
    // In NEVER mode the name is emitted bare, so any character that ends a bare identifier and
    // lets following text be parsed as SQL must be rejected:
    //  - any identifier or string quote character (not only this dialect's: e.g. '"' starts a
    //    string literal on MySQL without ANSI_QUOTES), since it opens a new token;
    //  - whitespace, because "x INT DEFAULT f()" would add a column definition or clause;
    //  - '(' ')' ',' because they close the column list or add further definitions.
    // None of these can appear in a working unquoted identifier, so no running connector that
    // produces valid SQL today is affected. Dots and single hyphens are left alone: a dot is the
    // qualifier delimiter and a lone '-' cannot add a statement or a clause.
    quoteIdentifiers = QuoteMethod.NEVER;
    String[] dangerous = {"a\"b", "a`b", "a'b", "a[b", "a]b", "a b", "a\u00a0b", "x INT",
        "a(b", "a)b", "a,b", "x INT); DROP TABLE t; --"};
    for (String name : dangerous) {
      assertRejected(name, b -> b.appendColumnName(name));
      assertRejected(name, b -> b.appendTableName(name));
      assertRejected(name, b -> b.appendIdentifier(name, QuoteMethod.NEVER));
    }
    assertExpression("my_col1", b -> b.appendColumnName("my_col1"));
    assertExpression("Col$1", b -> b.appendColumnName("Col$1"));
    assertExpression("caf\u00e9", b -> b.appendColumnName("caf\u00e9"));
    assertExpression("a-b", b -> b.appendColumnName("a-b"));
    assertExpression("c.s.t1", b -> b.append(TABLE_ID_T1));
  }

  @Test
  public void shouldRejectDialectContributedSequencesOnlyWhenUnquoted() {
    // A dialect can add sequences that end or hide the statement in its own SQL grammar, such as
    // MySQL's '#' line comment. They are rejected only in bare identifiers.
    quoteIdentifiers = QuoteMethod.NEVER;
    assertRejected("x#y", b -> b.setExtraForbiddenUnquotedSequences(Collections.singleton("#"))
        .appendColumnName("x#y"));
    assertRejected("x#y", b -> b.setExtraForbiddenUnquotedSequences(Collections.singleton("#"))
        .appendTableName("x#y"));
    quoteIdentifiers = QuoteMethod.ALWAYS;
    rules = new IdentifierRules(".", "`", "`");
    assertExpression("`x#y`", b -> b.setExtraForbiddenUnquotedSequences(
        Collections.singleton("#")).appendColumnName("x#y"));
  }

  @Test
  public void shouldAcceptHashInUnquotedIdentifiersByDefault() {
    // '#' is an ordinary identifier character in e.g. Oracle and SQL Server, so it is only
    // rejected for dialects that opt in.
    quoteIdentifiers = QuoteMethod.NEVER;
    assertExpression("x#y", b -> b.appendColumnName("x#y"));
    assertExpression("x#y", b -> b.setExtraForbiddenUnquotedSequences(null)
        .appendColumnName("x#y"));
  }

  @Test
  public void shouldTreatUnsupportedQuoteAsUnquoted() {
    // When the database reports that it does not support quoted identifiers the "quote" is a
    // space, so names are effectively bare and must be validated as in NEVER mode.
    rules = new IdentifierRules(".", IdentifierRules.UNSUPPORTED_QUOTE,
        IdentifierRules.UNSUPPORTED_QUOTE);
    assertRejected("x INT", b -> b.appendColumnName("x INT"));
    assertExpression(" ok ", b -> b.appendColumnName("ok"));
  }

  @Test
  public void shouldDoubleSingleQuoteInStringLiterals() {
    assertExpression("'a''b'", b -> b.appendStringQuoted("a'b"));
    assertExpression("'n/a'", b -> b.appendStringQuoted("n/a"));
    assertExpression("'x'');DROP TABLE t;--'", b -> b.appendStringQuoted("x');DROP TABLE t;--"));
    // Standard SQL: backslash is an ordinary character and is left untouched.
    assertExpression("'a\\b'", b -> b.appendStringQuoted("a\\b"));
  }

  @Test
  public void shouldDoubleBackslashForBackslashEscapingDialects() {
    Consumer<ExpressionBuilder> append = b -> b
        .setStringLiteralEscaping(ExpressionBuilder.StringLiteralEscaping.BACKSLASH)
        .appendStringQuoted("a\\'); DROP TABLE t; --");
    assertExpression("'a\\\\''); DROP TABLE t; --'", append);
  }

  @Test
  public void shouldUseEscapeStringSyntaxOnlyWhenBackslashPresent() {
    assertExpression("E'a\\\\''); DROP TABLE t; --'", b -> b
        .setStringLiteralEscaping(ExpressionBuilder.StringLiteralEscaping.ESCAPE_STRING_SYNTAX)
        .appendStringQuoted("a\\'); DROP TABLE t; --"));
    assertExpression("'O''Neill'", b -> b
        .setStringLiteralEscaping(ExpressionBuilder.StringLiteralEscaping.ESCAPE_STRING_SYNTAX)
        .appendStringQuoted("O'Neill"));
  }

  @Test
  public void displayFormsDoNotValidateIdentifiers() {
    // TableId/ColumnId string forms are used for logging and regex matching, not SQL; they must
    // not throw for unusual names discovered from database metadata.
    TableId odd = new TableId(null, "s;x", "t b");
    assertEquals("\"s;x\".\"t b\"", odd.toString());
    assertEquals("\"s;x\".\"t b\".\"c--d\"", new ColumnId(odd, "c--d").toString());
  }

  @Test
  public void disablingValidationAllowsAnyNameButKeepsQuoteDoubling() {
    // For identity strings that are never executed (offset keys, table matching, logging).
    quoteIdentifiers = QuoteMethod.NEVER;
    assertExpression("my table(1)", b -> b.setValidateIdentifiers(false)
        .appendTableName("my table(1)"));
    quoteIdentifiers = QuoteMethod.ALWAYS;
    assertExpression("\"a\"\"b c\"", b -> b.setValidateIdentifiers(false)
        .appendTableName("a\"b c"));
    // Validation stays on by default and can be re-enabled.
    quoteIdentifiers = QuoteMethod.NEVER;
    assertRejected("my table", b -> b.appendTableName("my table"));
    assertRejected("my table", b -> b.setValidateIdentifiers(false).setValidateIdentifiers(true)
        .appendTableName("my table"));
  }

  /**
   * Lex the quoted identifier the way a database does (a doubled closing quote is a literal
   * quote) and assert the identifier only ends at the final character, so nothing after it can
   * be parsed as SQL.
   */
  private void assertQuotedIdentifierClosesOnlyAtEnd(String name) {
    ExpressionBuilder builder = builderWith(rules);
    builder.appendColumnName(name);
    String quoted = builder.toString();
    assertEquals('"', quoted.charAt(0));
    int i = 1;
    while (i < quoted.length()) {
      if (quoted.charAt(i) == '"') {
        if (i + 1 < quoted.length() && quoted.charAt(i + 1) == '"') {
          i += 2;
          continue;
        }
        break;
      }
      i++;
    }
    assertEquals("identifier closed early in " + quoted, quoted.length() - 1, i);
  }

  private void assertRejected(String name, Consumer<ExpressionBuilder> builderFunction) {
    ExpressionBuilder builder = builderWith(rules);
    try {
      builderFunction.accept(builder);
      fail("Expected identifier to be rejected: " + name + " -> " + builder);
    } catch (ConnectException expected) {
      // expected
    }
  }

  protected void assertExpression(String expected, Consumer<ExpressionBuilder> builderFunction) {
    ExpressionBuilder builder = builderWith(rules);
    builderFunction.accept(builder);
    String actual = builder.toString();
    assertEquals(expected, actual);
  }

  protected ExpressionBuilder builderWith(IdentifierRules rules) {
    ExpressionBuilder result = new ExpressionBuilder(rules);
    result.setQuoteIdentifiers(quoteIdentifiers);
    return result;
  }

}