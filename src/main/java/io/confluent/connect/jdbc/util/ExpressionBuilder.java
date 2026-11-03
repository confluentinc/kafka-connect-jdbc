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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DropOptions;

/**
 * A class that can be used to build SQL expressions. A builder can be created with
 * {@link IdentifierRules} that dictate the quote characters and identifier delimiter,
 * or it can be obtained directly from a {@link DatabaseDialect}
 * via the {@link DatabaseDialect#expressionBuilder()} method.
 *
 * <p>The following is a simple example of how an instance of this class might be used to build a
 * SQL expression, where {@code table} might be a {@link TableId} object and {@code options} is a
 * {@link DropOptions} instance:
 *
 * <pre>
 *   builder.append("DROP TABLE ");
 *   builder.append(table);
 *   if (options.cascade()) {
 *     builder.append(" CASCADE CONSTRAINTS");
 *   }
 *   String dropStatement = builder.toString();
 * </pre>
 * The resulting drop statement expression might then be:
 * <pre>
 *   DROP TABLE "myCatalog"."myTable" CASCADE CONSTRAINTS
 * </pre>
 * Note how the {@link TableId} elements are properly quoted using the {@link IdentifierRules}
 * that are passed to the builder's constructor.
 *
 * <p>This class is often used within a {@link DatabaseDialect} implementation to construct the
 * various select, insert, update, upsert, and delete statements without having to explicitly
 * deal with fully-qualified identifiers, quoting rules, sizes of lists, etc.
 */
public class ExpressionBuilder {

  /**
   * A functional interface for anything that can be appended to an expression builder.
   * This makes use of double-dispatch to allow implementations to customize the behavior,
   * yet have callers not care about the differences in behavior.
   */
  @FunctionalInterface
  public interface Expressable {

    /**
     * Append this object to the specified builder.
     *
     * @param builder the builder to use; may not be null
     * @param useQuotes whether quotes should be used for this object
     */
    void appendTo(
        ExpressionBuilder builder,
        boolean useQuotes
    );

    /**
     * Append this object to the specified builder.
     *
     * @param builder the builder to use; may not be null
     * @param useQuotes whether quotes should be used for this object
     */
    default void appendTo(
        ExpressionBuilder builder,
        QuoteMethod useQuotes
    ) {
      switch (useQuotes) {
        case ALWAYS:
          appendTo(builder, true);
          break;
        case NEVER:
        default:
          // do nothing
          break;
      }
    }
  }

  /**
   * A functional interface for a transformation that an expression builder might use when
   * appending one or more other objects.
   *
   * @param <T> the type of object to transform before appending.
   */
  @FunctionalInterface
  public interface Transform<T> {
    void apply(
        ExpressionBuilder builder,
        T input
    );
  }

  /**
   * A fluent API interface returned by the {@link ExpressionBuilder#appendList()} method that
   * allows a caller to easily define a custom delimiter to be used between items in the list,
   * an optional transformation that should be applied to each item in the list, and the
   * items in the list. This is very handle when the number of items is not known a priori.
   *
   * @param <T> the type of object to be appended to the expression builder
   */
  public interface ListBuilder<T> {

    /**
     * Define the delimiter to appear between items in the list. If not specified, a comma
     * is used as the default delimiter.
     *
     * @param delimiter the delimiter; may not be null
     * @return this builder to enable methods to be chained; never null
     */
    ListBuilder<T> delimitedBy(String delimiter);

    /**
     * Define a {@link Transform} that should be applied to every item in the list as it is
     * appended.
     *
     * @param transform the transform; may not be null
     * @return this builder to enable methods to be chained; never null
     * @param <R> the type of item to be transformed
     */
    <R> ListBuilder<R> transformedBy(Transform<R> transform);

    /**
     * Append to this list all of the items in the specified {@link Iterable}.
     *
     * @param objects the objects to be appended to the list
     * @return this builder to enable methods to be chained; never null
     */
    ExpressionBuilder of(Iterable<? extends T> objects);

    /**
     * Append to this list all of the items in the specified {@link Iterable} objects.
     *
     * @param objects1 the first collection of objects to be added to the list
     * @param objects2 a second collection of objects to be added to the list
     * @return this builder to enable methods to be chained; never null
     */
    default ExpressionBuilder of(Iterable<? extends T> objects1, Iterable<? extends T> objects2) {
      of(objects1);
      return of(objects2);
    }

    /**
     * Append to this list all of the items in the specified {@link Iterable} objects.
     *
     * @param objects1 the first collection of objects to be added to the list
     * @param objects2 a second collection of objects to be added to the list
     * @param objects3 a third collection of objects to be added to the list
     * @return this builder to enable methods to be chained; never null
     */
    default ExpressionBuilder of(
        Iterable<? extends T> objects1,
        Iterable<? extends T> objects2,
        Iterable<? extends T> objects3
    ) {
      of(objects1);
      of(objects2);
      return of(objects3);
    }
  }

  /**
   * Get a {@link Transform} that will surround the inputs with quotes.
   *
   * @return the transform; never null
   */
  public static Transform<String> quote() {
    return (builder, input) -> builder.appendColumnName(input);
  }

  /**
   * Get a {@link Transform} that will quote just the column names.
   *
   * @return the transform; never null
   */
  public static Transform<ColumnId> columnNames() {
    return (builder, input) -> builder.appendColumnName(input.name());
  }

  /**
   * Get a {@link Transform} that will quote just the column names and append the given string.
   *
   * @param appended the string to append after the quoted column names
   * @return the transform; never null
   */
  public static Transform<ColumnId> columnNamesWith(final String appended) {
    return (builder, input) -> {
      builder.appendColumnName(input.name());
      builder.append(appended);
    };
  }

  /**
   * Get a {@link Transform} that will append a placeholder rather than each of the column names.
   *
   * @param str the string to output instead the each column name
   * @return the transform; never null
   */
  public static Transform<ColumnId> placeholderInsteadOfColumnNames(final String str) {
    return (builder, input) -> builder.append(str);
  }

  /**
   * Get a {@link Transform} that will append the prefix and then the quoted column name.
   *
   * @param prefix the string to output before the quoted column names
   * @return the transform; never null
   */
  public static Transform<ColumnId> columnNamesWithPrefix(final String prefix) {
    return (builder, input) -> {
      builder.append(prefix);
      builder.appendColumnName(input.name());
    };
  }

  /**
   * Create a new ExpressionBuilder using the default {@link IdentifierRules}.
   *
   * @return the expression builder
   */
  public static ExpressionBuilder create() {
    return new ExpressionBuilder();
  }

  protected static final QuoteMethod DEFAULT_QUOTE_METHOD = QuoteMethod.ALWAYS;

  /**
   * How a dialect interprets backslash inside a single-quoted string literal, which determines how
   * {@link #appendStringQuoted(Object)} must escape a value so it cannot close the literal early.
   */
  public enum StringLiteralEscaping {
    /**
     * SQL standard: backslash is an ordinary character; only the single quote is doubled.
     */
    STANDARD,
    /**
     * Backslash is an escape character (for example MySQL without {@code NO_BACKSLASH_ESCAPES}),
     * so both the backslash and the single quote are doubled.
     */
    BACKSLASH,
    /**
     * Backslash may or may not be an escape character depending on server settings (PostgreSQL
     * {@code standard_conforming_strings}). Values containing a backslash are emitted as an
     * {@code E'...'} literal, in which backslash is always an escape, with both the backslash and
     * the single quote doubled; other values use a standard literal.
     */
    ESCAPE_STRING_SYNTAX
  }

  /**
   * Character sequences that can end, extend, or hide the remainder of a SQL statement. They are
   * rejected in bare (unquoted) identifiers; inside a quoted identifier they are ordinary
   * characters and the closing-quote doubling already keeps them contained.
   */
  private static final String[] FORBIDDEN_IDENTIFIER_SEQUENCES = {";", "--", "/*", "*/"};

  /**
   * Characters that terminate a bare (unquoted) identifier and let the following text be parsed as
   * SQL: identifier and string quotes of any dialect, and list/parenthesis punctuation. Whitespace
   * is handled separately. None of these can appear in a working unquoted identifier.
   */
  private static final String FORBIDDEN_UNQUOTED_IDENTIFIER_CHARS = "\"`'[](),";

  /**
   * Set how string literals appended via {@link #appendStringQuoted(Object)} are escaped.
   *
   * @param escaping the escaping style; may be null if {@link StringLiteralEscaping#STANDARD}
   *                 should be used
   * @return this expression builder; never null
   */
  public ExpressionBuilder setStringLiteralEscaping(StringLiteralEscaping escaping) {
    this.stringLiteralEscaping = escaping != null ? escaping : StringLiteralEscaping.STANDARD;
    return this;
  }

  /**
   * Set additional character sequences that must not appear in a bare (unquoted) identifier
   * because the dialect's SQL grammar gives them special meaning, for example MySQL's {@code #}
   * line comment. Quoted identifiers are unaffected, since quote-doubling already contains them.
   *
   * @param sequences the sequences to reject; may be null or empty for none
   * @return this expression builder; never null
   */
  public ExpressionBuilder setExtraForbiddenUnquotedSequences(Collection<String> sequences) {
    List<String> nonEmpty = new ArrayList<>();
    if (sequences != null) {
      for (String sequence : sequences) {
        if (sequence != null && !sequence.isEmpty()) {
          nonEmpty.add(sequence);
        }
      }
    }
    this.extraForbiddenUnquotedSequences = Collections.unmodifiableList(nonEmpty);
    return this;
  }

  /**
   * Set whether identifier names are validated before they are appended. Validation is on by
   * default and must stay on for any expression that is executed as SQL. Disable it only for
   * identity strings that are never executed, such as offset partition keys, table-matching keys
   * and log output, so that legitimate names discovered from the database do not fail. Quoting
   * and quote-doubling are unaffected.
   *
   * @param validate true to validate identifiers, false to append them without validation
   * @return this expression builder; never null
   */
  public ExpressionBuilder setValidateIdentifiers(boolean validate) {
    this.validateIdentifiers = validate;
    return this;
  }

  /**
   * Create a builder for human-readable forms of identifiers (logging, regex matching), which are
   * never executed as SQL and so must not fail on unusual names discovered from the database.
   */
  static ExpressionBuilder createForDisplay() {
    return new ExpressionBuilder().setValidateIdentifiers(false);
  }

  private final IdentifierRules rules;
  private final StringBuilder sb = new StringBuilder();
  private QuoteMethod quoteSqlIdentifiers = DEFAULT_QUOTE_METHOD;
  private StringLiteralEscaping stringLiteralEscaping = StringLiteralEscaping.STANDARD;
  private boolean validateIdentifiers = true;
  private List<String> extraForbiddenUnquotedSequences = Collections.emptyList();

  /**
   * Create a new expression builder with the default {@link IdentifierRules}.
   */
  public ExpressionBuilder() {
    this(null);
  }

  /**
   * Create a new expression builder that uses the specified {@link IdentifierRules}.
   *
   * @param rules the rules; may be null if the default rules are to be used
   */
  public ExpressionBuilder(IdentifierRules rules) {
    this.rules = rules != null ? rules : IdentifierRules.DEFAULT;
  }

  /**
   * Set when this expression builder should quote identifiers, such as table and column names.
   *
   * @param method the quoting method; may be null if the default method
   *               ({@link QuoteMethod#ALWAYS always}) should be used
   * @return this expression builder; never null
   */
  public ExpressionBuilder setQuoteIdentifiers(QuoteMethod method) {
    this.quoteSqlIdentifiers = method != null ? method : DEFAULT_QUOTE_METHOD;
    return this;
  }

  /**
   * Return a new ExpressionBuilder that escapes quotes with the specified prefix.
   * This builder remains unaffected.
   *
   * @param prefix the prefix
   * @return the new ExpressionBuilder, or this builder if the prefix is null or empty
   */
  public ExpressionBuilder escapeQuotesWith(String prefix) {
    if (prefix == null || prefix.isEmpty()) {
      return this;
    }
    return new ExpressionBuilder(this.rules.escapeQuotesWith(prefix));
  }

  /**
   * Append to this builder's expression the delimiter defined by this builder's
   * {@link IdentifierRules}.
   *
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendIdentifierDelimiter() {
    sb.append(rules.identifierDelimiter());
    return this;
  }

  /**
   * Always append to this builder's expression the leading quote character(s) defined by this
   * builder's {@link IdentifierRules}.
   *
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendLeadingQuote() {
    return appendLeadingQuote(QuoteMethod.ALWAYS);
  }


  protected ExpressionBuilder appendLeadingQuote(QuoteMethod method) {
    switch (method) {
      case ALWAYS:
        sb.append(rules.leadingQuoteString());
        break;
      case NEVER:
      default:
        break;
    }
    return this;
  }

  /**
   * Always append to this builder's expression the trailing quote character(s) defined by this
   * builder's {@link IdentifierRules}.
   *
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendTrailingQuote() {
    return appendTrailingQuote(QuoteMethod.ALWAYS);
  }

  protected ExpressionBuilder appendTrailingQuote(QuoteMethod method) {
    switch (method) {
      case ALWAYS:
        sb.append(rules.trailingQuoteString());
        break;
      case NEVER:
      default:
        break;
    }
    return this;
  }

  /**
   * Append to this builder's expression the string quote character ({@code '}).
   *
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendStringQuote() {
    sb.append("'");
    return this;
  }

  /**
   * Append to this builder's expression a string surrounded by single quote characters ({@code '}).
   * Embedded single quotes (and, where the dialect treats it as an escape, backslashes) are
   * escaped so the value cannot terminate the literal; see {@link StringLiteralEscaping}.
   * Use {@link #appendIdentifier(String, QuoteMethod)} for identifiers,
   * {@link #appendColumnName(String, QuoteMethod)} for column names, or
   * {@link #appendTableName(String, QuoteMethod)} for table names.
   *
   * @param name the object whose string representation is to be appended
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendStringQuoted(Object name) {
    String value = String.valueOf(name);
    boolean escapeBackslash = stringLiteralEscaping == StringLiteralEscaping.BACKSLASH
        || (stringLiteralEscaping == StringLiteralEscaping.ESCAPE_STRING_SYNTAX
            && value.indexOf('\\') >= 0);
    if (escapeBackslash) {
      value = value.replace("\\", "\\\\");
      if (stringLiteralEscaping == StringLiteralEscaping.ESCAPE_STRING_SYNTAX) {
        sb.append('E');
      }
    }
    appendStringQuote();
    sb.append(value.replace("'", "''"));
    appendStringQuote();
    return this;
  }

  /**
   * Append to this builder's expression the identifier.
   *
   * @param name the name to be appended
   * @param quoted true if the name should be quoted, or false otherwise
   * @return this builder to enable methods to be chained; never null
   * @deprecated use {@link #appendIdentifier(String, QuoteMethod)} instead
   */
  @Deprecated
  public ExpressionBuilder appendIdentifier(
      String name,
      boolean quoted
  ) {
    return appendIdentifier(name, quoted ? QuoteMethod.ALWAYS : QuoteMethod.NEVER);
  }

  /**
   * Append to this builder's expression the identifier.
   *
   * @param name the name to be appended
   * @param quoted true if the name should be quoted, or false otherwise
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendIdentifier(
      String name,
      QuoteMethod quoted
  ) {
    return appendIdentifierText(name, quoted);
  }

  /**
   * Append to this builder's expression the specified Column identifier, possibly surrounded by
   * the leading and trailing quotes based upon {@link #setQuoteIdentifiers(QuoteMethod)}.
   *
   * @param name the name to be appended
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendTableName(String name) {
    return appendTableName(name, quoteSqlIdentifiers);
  }

  /**
   * Append to this builder's expression the specified Column identifier, possibly surrounded by
   * the leading and trailing quotes based upon {@link #setQuoteIdentifiers(QuoteMethod)}.
   *
   * @param name the name to be appended
   * @param quote the quote method to be used
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendTableName(String name, QuoteMethod quote) {
    return appendIdentifierText(name, quote);
  }

  /**
   * Append to this builder's expression the specified Column identifier, possibly surrounded by
   * the leading and trailing quotes based upon {@link #setQuoteIdentifiers(QuoteMethod)}.
   *
   * @param name the name to be appended
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendColumnName(String name) {
    return appendColumnName(name, quoteSqlIdentifiers);
  }

  /**
   * Append to this builder's expression the specified Column identifier, possibly surrounded by
   * the leading and trailing quotes based upon {@link #setQuoteIdentifiers(QuoteMethod)}.
   *
   * @param name the name to be appended
   * @param quote whether to quote the column name; may not be null
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendColumnName(String name, QuoteMethod quote) {
    return appendIdentifierText(name, quote);
  }

  /**
   * Append to this builder's expression the specified identifier, surrounded by the leading and
   * trailing quotes.
   *
   * @param name the name to be appended
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendIdentifierQuoted(String name) {
    return appendIdentifierText(name, QuoteMethod.ALWAYS);
  }

  /**
   * Append an identifier so that untrusted text cannot break out of it or extend the statement.
   *
   * <p>When the identifier is quoted, an embedded trailing-quote string is escaped by doubling it
   * (for example {@code "} becomes {@code ""}, and for bracket quoting {@code ]} becomes
   * {@code ]]}). Quote doubling cannot protect a bare identifier, so the name is also validated:
   * see {@link #validateIdentifier(String, boolean)}.
   */
  private ExpressionBuilder appendIdentifierText(String name, QuoteMethod quote) {
    boolean quoted = quote == QuoteMethod.ALWAYS && supportsQuotedIdentifiers();
    if (name != null && validateIdentifiers) {
      validateIdentifier(name, quoted);
    }
    appendLeadingQuote(quote);
    if (quoted && name != null) {
      String trailing = rules.trailingQuoteString();
      sb.append(name.replace(trailing, trailing + trailing));
    } else {
      sb.append(name);
    }
    appendTrailingQuote(quote);
    return this;
  }

  private boolean supportsQuotedIdentifiers() {
    String trailing = rules.trailingQuoteString();
    return !trailing.isEmpty() && !trailing.equals(IdentifierRules.UNSUPPORTED_QUOTE);
  }

  /**
   * Reject identifiers that could end, extend, or hide the rest of the generated statement.
   * Control characters are rejected in every mode as defense in depth. A quoted identifier is
   * otherwise contained by doubling its closing quote. A bare (unquoted) identifier additionally
   * may not contain the statement separator, SQL comment introducers (including any the dialect
   * adds, see {@link #setExtraForbiddenUnquotedSequences(Collection)}), whitespace, quote
   * characters, parentheses, or commas, since any of those would let the remaining text be parsed
   * as SQL. Ordinary names such as {@code [A-Za-z_][A-Za-z0-9_]*} are never affected.
   */
  private void validateIdentifier(String name, boolean quoted) {
    if (!quoted) {
      rejectContainedSequences(name, Arrays.asList(FORBIDDEN_IDENTIFIER_SEQUENCES));
      rejectContainedSequences(name, extraForbiddenUnquotedSequences);
    }
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (Character.isISOControl(c)) {
        throw invalidIdentifier(name, String.format("control character U+%04X", (int) c));
      }
      if (!quoted) {
        validateUnquotedIdentifierChar(name, c);
      }
    }
  }

  private static void rejectContainedSequences(String name, List<String> sequences) {
    for (String sequence : sequences) {
      if (name.contains(sequence)) {
        throw invalidIdentifier(name, "'" + sequence + "'");
      }
    }
  }

  private void validateUnquotedIdentifierChar(String name, char c) {
    if (Character.isWhitespace(c) || Character.isSpaceChar(c)) {
      throw invalidIdentifier(name, "whitespace");
    }
    if (FORBIDDEN_UNQUOTED_IDENTIFIER_CHARS.indexOf(c) >= 0
        || rules.leadingQuoteString().indexOf(c) >= 0
        || rules.trailingQuoteString().indexOf(c) >= 0) {
      throw invalidIdentifier(name, "'" + c + "'");
    }
  }

  private static ConnectException invalidIdentifier(String name, String reason) {
    StringBuilder printable = new StringBuilder(name.length());
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      printable.append(Character.isISOControl(c) ? '?' : c);
    }
    return new ConnectException(String.format(
        "Identifier '%s' contains %s, which is not allowed in a generated SQL identifier because "
        + "it could alter the statement. Rename the field or table.",
        printable, reason));
  }

  /**
   * Append to this builder's expression the binary value as a hex string, prefixed and
   * suffixed by a single quote character.
   *
   * @param value the value to be appended
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendBinaryLiteral(byte[] value) {
    return append("x'").append(BytesUtil.toHex(value)).append("'");
  }

  /**
   * Append to this builder's expression a new line.
   *
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder appendNewLine() {
    sb.append(System.lineSeparator());
    return this;
  }

  /**
   * Append to this builder's expression the specified object. If the object is {@link Expressable},
   * then this builder delegates to the object's
   * {@link Expressable#appendTo(ExpressionBuilder, boolean)} method. Otherwise, the string
   * representation of the object is appended to the expression.
   *
   * @param obj the object to be appended
   * @param useQuotes true if the object should be surrounded by quotes, or false otherwise
   * @return this builder to enable methods to be chained; never null
   * @deprecated use {@link #append(Object, QuoteMethod)} instead
   */
  @Deprecated
  public ExpressionBuilder append(
      Object obj,
      boolean useQuotes
  ) {
    return append(obj, useQuotes ? QuoteMethod.ALWAYS : QuoteMethod.NEVER);
  }

  /**
   * Append to this builder's expression the specified object. If the object is {@link Expressable},
   * then this builder delegates to the object's
   * {@link Expressable#appendTo(ExpressionBuilder, boolean)} method. Otherwise, the string
   * representation of the object is appended to the expression.
   *
   * @param obj the object to be appended
   * @param useQuotes true if the object should be surrounded by quotes, or false otherwise
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder append(
      Object obj,
      QuoteMethod useQuotes
  ) {
    if (obj instanceof Expressable) {
      ((Expressable) obj).appendTo(this, useQuotes);
    } else if (obj != null) {
      sb.append(obj);
    }
    return this;
  }

  /**
   * Append to this builder's expression the specified object surrounded by quotes. If the object
   * is {@link Expressable}, then this builder delegates to the object's
   * {@link Expressable#appendTo(ExpressionBuilder, boolean)} method. Otherwise, the string
   * representation of the object is appended to the expression.
   *
   * @param obj the object to be appended
   * @return this builder to enable methods to be chained; never null
   */
  public ExpressionBuilder append(Object obj) {
    return append(obj, quoteSqlIdentifiers);
  }

  /**
   * Append to this builder's expression the specified object surrounded by quotes. If the object
   * is {@link Expressable}, then this builder delegates to the object's
   * {@link Expressable#appendTo(ExpressionBuilder, boolean)} method. Otherwise, the string
   * representation of the object is appended to the expression.
   *
   * @param obj the object to be appended
   * @param transform the transform that should be used on the supplied object to obtain the
   *                  representation that is appended to the expression; may be null
   * @param <T> the type of object to transform before appending.
   *
   * @return this builder to enable methods to be chained; never null
   */
  public <T> ExpressionBuilder append(
      T obj,
      Transform<T> transform
  ) {
    if (transform != null) {
      transform.apply(this, obj);
    } else {
      append(obj);
    }
    return this;
  }

  protected class BasicListBuilder<T> implements ListBuilder<T> {
    private final String delimiter;
    private final Transform<T> transform;
    private boolean first = true;

    BasicListBuilder() {
      this(", ", null);
    }

    BasicListBuilder(String delimiter, Transform<T> transform) {
      this.delimiter = delimiter;
      this.transform = transform != null ? transform : ExpressionBuilder::append;
    }

    @Override
    public ListBuilder<T> delimitedBy(String delimiter) {
      return new BasicListBuilder<T>(delimiter, transform);
    }

    @Override
    public <R> ListBuilder<R> transformedBy(Transform<R> transform) {
      return new BasicListBuilder<>(delimiter, transform);
    }

    @Override
    public ExpressionBuilder of(Iterable<? extends T> objects) {
      for (T obj : objects) {
        if (first) {
          first = false;
        } else {
          append(delimiter);
        }
        append(obj, transform);
      }
      return ExpressionBuilder.this;
    }
  }

  public ListBuilder<Object> appendList() {
    return new BasicListBuilder<>();
  }

  public ExpressionBuilder appendMultiple(
      String delimiter,
      String expression,
      int times
  ) {
    for (int i = 0; i < times; i++) {
      if (i > 0) {
        append(delimiter);
      }
      append(expression);
    }
    return this;
  }

  public String toString() {
    return sb.toString();
  }
}
