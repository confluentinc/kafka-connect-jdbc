/*
 * Copyright 2026 Confluent Inc.
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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sanitizes JDBC sink error messages while preserving diagnostic structure.
 *
 * <p>The grammar recognizes specific pgjdbc, MySQL, and SQL Server message shapes. It falls closed
 * when a recognized shape is incomplete or a batch statement shape is unknown.
 */
final class SqlErrorMessageSanitizer {
  static final String REDACTED_VALUE = "<redacted>";

  private static final Pattern VALUE_REGION =
      Pattern.compile("\\s(VALUES\\s*\\(|SET\\s|WHERE\\s)", Pattern.CASE_INSENSITIVE);
  private static final Pattern BATCH_SAFE_HEAD =
      Pattern.compile("^\\s*Batch\\s+entry\\s+\\d+\\s+[A-Za-z]+", Pattern.CASE_INSENSITIVE);
  private static final String ABORTED_MARKER = " was aborted";
  private static final String ERROR_MARKER = ": ERROR:";

  private static final Pattern[] REASON_END_MARKERS = {
      Pattern.compile("\\n\\s*Detail:"),
      Pattern.compile("\\n\\s*Hint:"),
      Pattern.compile("\\n\\s*Where:"),
      Pattern.compile("\\n\\s*Position:"),
      Pattern.compile("\\s{2}Call getNextException"),
  };

  private static final Pattern DETAIL_FAILING_ROW_PREFIX =
      Pattern.compile("Detail:\\s*Failing row contains\\s*\\(", Pattern.CASE_INSENSITIVE);
  private static final Pattern DETAIL_KEY_PREFIX =
      Pattern.compile(
          "Detail:\\s*Key\\s*\\([^\\r\\n)]*\\)\\s*=\\s*\\(",
          Pattern.CASE_INSENSITIVE
      );
  private static final Pattern DUPLICATE_KEY_VALUE_PREFIX =
      Pattern.compile("The duplicate key value is\\s*\\(", Pattern.CASE_INSENSITIVE);
  private static final Pattern PAREN_VALUE_END =
      Pattern.compile("\\)\\.|\\)(?=\\s*$)");
  private static final Pattern DETAIL_KEY_VALUE_END =
      Pattern.compile(
          "\\)\\s+(?:already exists"
              + "|is not present in table\\s+\"[^\\r\\n]*\""
              + "|is still referenced from table\\s+\"[^\\r\\n]*\")",
          Pattern.CASE_INSENSITIVE
      );

  private static final Pattern MYSQL_DUPLICATE_ENTRY_PREFIX =
      Pattern.compile("Duplicate\\s+entry\\s+'", Pattern.CASE_INSENSITIVE);
  private static final Pattern MYSQL_DUPLICATE_ENTRY_END =
      Pattern.compile("'\\s+for\\s+key\\s+(?=')", Pattern.CASE_INSENSITIVE);
  private static final Pattern MYSQL_COLUMN_VALUE_PREFIX =
      Pattern.compile("\\bvalue\\s*:\\s*'", Pattern.CASE_INSENSITIVE);
  private static final Pattern MYSQL_COLUMN_VALUE_END =
      Pattern.compile("'\\s+for\\s+column\\s+(?=')", Pattern.CASE_INSENSITIVE);

  private static final Pattern POSTGRES_DOUBLE_QUOTED_VALUE_PREFIX =
      Pattern.compile(
          "(?:invalid input syntax for type|invalid input value for enum)"
              + "\\s+[^:\\r\\n]+:\\s*\"",
          Pattern.CASE_INSENSITIVE
      );
  private static final Pattern DOUBLE_QUOTE_END =
      Pattern.compile(
          "\"(?=\\s*(?:$|\\r?\\n|in\\s+column\\b|of\\s+relation\\b|at\\s+character\\b))",
          Pattern.CASE_INSENSITIVE
      );

  private static final List<ValueGroupRule> VALUE_GROUP_RULES = Arrays.asList(
      new ValueGroupRule(
          MYSQL_DUPLICATE_ENTRY_PREFIX,
          MYSQL_DUPLICATE_ENTRY_END,
          true
      ),
      new ValueGroupRule(
          MYSQL_COLUMN_VALUE_PREFIX,
          MYSQL_COLUMN_VALUE_END,
          true
      ),
      new ValueGroupRule(
          DETAIL_FAILING_ROW_PREFIX,
          PAREN_VALUE_END,
          true
      ),
      new ValueGroupRule(
          DETAIL_KEY_PREFIX,
          DETAIL_KEY_VALUE_END,
          true
      ),
      new ValueGroupRule(
          DUPLICATE_KEY_VALUE_PREFIX,
          PAREN_VALUE_END,
          true
      ),
      new ValueGroupRule(
          POSTGRES_DOUBLE_QUOTED_VALUE_PREFIX,
          DOUBLE_QUOTE_END,
          false
      )
  );

  private static final String OPENING_QUOTE_DELIMITER_PUNCTUATION = "(=,:";
  private static final String[] VALUE_LITERAL_PREFIXES = {"_utf8mb4", "N", "E", "B", "x"};

  // Drivers can single-quote both values and identifiers. Only explicitly introduced identifiers
  // are retained; unknown single-quoted tokens default to redaction.
  private static final Pattern KEEP_IDENTIFIER_PREFIX =
      Pattern.compile(
          "(?:for\\s+key|for\\s+column|constraint|object)\\s+$",
          Pattern.CASE_INSENSITIVE
      );

  private SqlErrorMessageSanitizer() {
  }

  static String sanitize(String message) {
    if (message == null) {
      return null;
    }
    String sanitized = sanitizeBatchStatement(message);
    if (sanitized == null) {
      sanitized = message;
    }
    sanitized = sanitizeValueGroups(sanitized);
    return sanitizeSingleQuotedValues(sanitized);
  }

  private static String sanitizeBatchStatement(String message) {
    if (!message.trim().startsWith("Batch entry")) {
      return null;
    }
    int abortedIndex = message.indexOf(ABORTED_MARKER);
    if (abortedIndex < 0) {
      return null;
    }

    Matcher valueRegion = VALUE_REGION.matcher(message);
    String head;
    if (valueRegion.find() && valueRegion.start() < abortedIndex) {
      head = message.substring(0, valueRegion.start());
    } else {
      Matcher safeHead = BATCH_SAFE_HEAD.matcher(message);
      head = safeHead.find() && safeHead.end() <= abortedIndex
          ? message.substring(0, safeHead.end())
          : "Batch entry";
    }

    int errorIndex = message.indexOf(ERROR_MARKER, abortedIndex);
    if (errorIndex < 0) {
      return head;
    }
    return head + boundedReason(message, errorIndex);
  }

  private static String boundedReason(String message, int start) {
    int end = -1;
    for (Pattern pattern : REASON_END_MARKERS) {
      Matcher matcher = pattern.matcher(message);
      if (matcher.find(start) && (end < 0 || matcher.start() < end)) {
        end = matcher.start();
      }
    }
    return end < 0 ? "" : message.substring(start, end);
  }

  private static String sanitizeValueGroups(String message) {
    String sanitized = message;
    for (ValueGroupRule rule : VALUE_GROUP_RULES) {
      sanitized = rule.sanitize(sanitized);
    }
    return sanitized;
  }

  private static String sanitizeBoundedValues(
      String message,
      Pattern prefixPattern,
      Pattern endPattern,
      boolean useRightmostEnd
  ) {
    Matcher prefix = prefixPattern.matcher(message);
    if (!prefix.find()) {
      return message;
    }

    StringBuilder sanitized = new StringBuilder(message.length());
    int copyFrom = 0;
    do {
      sanitized.append(message, copyFrom, prefix.end()).append(REDACTED_VALUE);
      Matcher end = endPattern.matcher(message).region(prefix.end(), message.length());
      if (!end.find()) {
        return sanitized.toString();
      }
      int endStart = end.start();
      int endEnd = end.end();
      if (useRightmostEnd) {
        while (end.find()) {
          endStart = end.start();
          endEnd = end.end();
        }
      }
      sanitized.append(message, endStart, endEnd);
      copyFrom = endEnd;
    } while (copyFrom < message.length() && prefix.find(copyFrom));

    return sanitized.append(message, copyFrom, message.length()).toString();
  }

  private static String sanitizeSingleQuotedValues(String message) {
    StringBuilder sanitized = new StringBuilder(message.length());
    int index = 0;
    while (index < message.length()) {
      char current = message.charAt(index);
      if (current == '\'' && isOpeningQuoteContext(message, index)) {
        int closingQuote = findClosingQuote(message, index + 1);
        if (closingQuote >= 0) {
          if (isKeptIdentifierQuote(message, index)) {
            sanitized.append(message, index, closingQuote + 1);
          } else {
            sanitized.append('\'').append(REDACTED_VALUE).append('\'');
          }
          index = closingQuote + 1;
          continue;
        }
        sanitized.append('\'').append(REDACTED_VALUE);
        break;
      }
      sanitized.append(current);
      index++;
    }
    return sanitized.toString();
  }

  private static boolean isKeptIdentifierQuote(String message, int quoteIndex) {
    int literalPrefixStart = literalPrefixStart(message, quoteIndex);
    int identifierPrefixEnd = literalPrefixStart >= 0 ? literalPrefixStart : quoteIndex;
    return KEEP_IDENTIFIER_PREFIX.matcher(message).region(0, identifierPrefixEnd).find();
  }

  private static boolean isOpeningQuoteContext(String message, int index) {
    if (index == 0) {
      return true;
    }
    return isOpeningQuoteDelimiter(message.charAt(index - 1))
        || literalPrefixStart(message, index) >= 0;
  }

  private static boolean isOpeningQuoteDelimiter(char current) {
    return Character.isWhitespace(current)
        || OPENING_QUOTE_DELIMITER_PUNCTUATION.indexOf(current) >= 0;
  }

  private static int literalPrefixStart(String message, int quoteIndex) {
    for (String prefix : VALUE_LITERAL_PREFIXES) {
      int start = quoteIndex - prefix.length();
      if (start < 0
          || !message.regionMatches(true, start, prefix, 0, prefix.length())) {
        continue;
      }
      if (start == 0 || isLiteralPrefixBoundary(message.charAt(start - 1))) {
        return start;
      }
    }
    return -1;
  }

  private static boolean isLiteralPrefixBoundary(char current) {
    return !Character.isLetterOrDigit(current) && current != '_';
  }

  private static int findClosingQuote(String message, int start) {
    int index = start;
    while (index < message.length()) {
      char current = message.charAt(index);
      if (current == '\\'
          && index + 1 < message.length()
          && message.charAt(index + 1) == '\'') {
        index += 2;
        continue;
      }
      if (current == '\'') {
        if (index + 1 < message.length() && message.charAt(index + 1) == '\'') {
          index += 2;
          continue;
        }
        return index;
      }
      index++;
    }
    return -1;
  }

  private static final class ValueGroupRule {
    private final Pattern prefixPattern;
    private final Pattern endPattern;
    private final boolean useRightmostEnd;

    private ValueGroupRule(
        Pattern prefixPattern,
        Pattern endPattern,
        boolean useRightmostEnd
    ) {
      this.prefixPattern = prefixPattern;
      this.endPattern = endPattern;
      this.useRightmostEnd = useRightmostEnd;
    }

    private String sanitize(String message) {
      return sanitizeBoundedValues(
          message,
          prefixPattern,
          endPattern,
          useRightmostEnd
      );
    }
  }
}
