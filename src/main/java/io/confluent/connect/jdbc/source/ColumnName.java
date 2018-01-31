/**
 * Copyright 2015 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.util.JdbcUtils;
import io.confluent.connect.jdbc.util.StringUtils;

import java.util.Objects;

/**
 * Represents a potentially qualified column name and facilitates quoting the name at every level.
 * </p>
 * A ColumnName is considered empty if the name is blank (mull, empty or whitespace only)
 */
public class ColumnName {
  private final String name;
  private final String qualifier;


  public static ColumnName empty() {
    return new ColumnName(null, null);
  }


  public ColumnName(final String name, final String qualifier) {
    this.name = name;
    this.qualifier = qualifier;
  }


  public String getName() {
    return name;
  }


  public String getQualifier() {
    return qualifier;
  }


  /**
   * Get the fully qualified name quoted at every level with the given quote string.
   */
  public String getQuotedQualifiedName(String quoteString) {
    String qualifiedAndQuoted = JdbcUtils.quoteString(name, quoteString);
    if (!StringUtils.isBlank(qualifier)) {
      qualifiedAndQuoted = JdbcUtils.quoteString(qualifier, quoteString) + "." + qualifiedAndQuoted;
    }

    return qualifiedAndQuoted;
  }


  public boolean isEmpty() {
    return StringUtils.isBlank(name);
  }


  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ColumnName that = (ColumnName) o;
    return Objects.equals(name, that.name) && Objects.equals(qualifier, that.qualifier);
  }


  @Override
  public int hashCode() {
    return Objects.hash(name, qualifier);
  }


  @Override
  public String toString() {
    return "ColumnName{" + "name='" + name + '\'' + ", qualifier='" + qualifier + '\'' + '}';
  }
}
