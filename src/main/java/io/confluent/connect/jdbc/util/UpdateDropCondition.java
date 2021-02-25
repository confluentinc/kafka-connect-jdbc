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

import io.confluent.connect.jdbc.util.ExpressionBuilder.Expressable;

import java.util.Objects;

public class UpdateDropCondition implements Expressable {

  private final ColumnId field;
  private final String operator;
  private final int hash;

  public UpdateDropCondition(String field) {
    this(new ColumnId(null, field.trim(), field.trim()));
  }

  public UpdateDropCondition(ColumnId field) {
    this.field = field;
    this.operator = "<";
    this.hash = Objects.hash(this.field.name(), this.operator);
  }

  public ColumnId field() {
    return this.field;
  }

  public String operator() {
    return this.operator;
  }

  @Override
  public void appendTo(ExpressionBuilder builder, boolean useQuotes) {
    appendTo(builder, useQuotes ? QuoteMethod.ALWAYS : QuoteMethod.NEVER);
  }

  @Override
  public void appendTo(
      ExpressionBuilder builder,
      QuoteMethod useQuotes
  ) {
    builder.appendColumnName(this.field.name(), useQuotes);
    builder.append(this.operator);
    builder.appendColumnName(this.field.name(), useQuotes);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof UpdateDropCondition) {
      UpdateDropCondition that = (UpdateDropCondition) obj;
      return Objects.equals(this.field.name(), that.field.name())
             && Objects.equals(this.operator, that.operator);
    }
    return false;
  }

  @Override
  public String toString() {
    return ExpressionBuilder.create().append(this).toString();
  }
}
