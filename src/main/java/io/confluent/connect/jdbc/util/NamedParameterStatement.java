/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Calendar;

/**
 * This class wraps around a {@link PreparedStatement} and allows the programmer to set
 * parameters by name instead of by index
 *
 *<p>Allows to replace a writing of this type:
 *
 *<p>String query = "select * from table where name =? Or address =?";
 * PreparedStatement ps = conn.prepareStatement (query);
 * ps.setString (1, "jose");
 * p.setString (2, "address name");
 * ResultSet rs = p.executeQuery ();
 *
 *<p>with one of this other, much more readable:
 *
 *<p>String query = "select * from table where name = :name or address = :address";
 * NamedParameterStatement nps = new NamedParameterStatement (conn, query);
 * nps.setString ("name", "jose");
 * nps.setString ("address", "address name");
 * ResultSet rs = nps.executeQuery ();
 */
public class NamedParameterStatement {

  /**
   * The statement this object is wrapping.
   */
  private final PreparedStatement statement;

  /**
   * Maps parameter names to arrays of ints which are the parameter indices.
   */
  private final Map indexMap;

  /**
   * Creates a NamedParameterStatement. Wraps a call to
   * c.{@link Connection#prepareStatement(java.lang.String) prepareStatement}.
   *
   * @param connection the database connection
   * @param query      the parameterized query
   * @throws SQLException if the statement could not be created
   */
  public NamedParameterStatement(Connection connection, String query) throws SQLException {
    indexMap = new HashMap();
    String parsedQuery = parse(query, indexMap);
    statement = connection.prepareStatement(parsedQuery);
  }

  /**
   * Parses a query with named parameters. The parameter-index mappings are put into
   * the map, and the parsed query is returned.
   * DO NOT CALL FROM CLIENT CODE. This method is non-private so JUnit code can test it.
   *
   * @param query    query to parse
   * @param paramMap map to hold parameter-index mappings
   * @return the parsed query
   */
  static final String parse(String query, Map paramMap) {
    // I was originally using regular expressions, but they didn't work well for ignoring
    // parameter-like strings inside quotes.
    int length = query.length();
    StringBuffer parsedQuery = new StringBuffer(length);
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    int index = 1;

    for (int i = 0; i < length; i++) {
      char c = query.charAt(i);
      if (inSingleQuote) {
        if (c == '\'') {
          inSingleQuote = false;
        }
      } else if (inDoubleQuote) {
        if (c == '"') {
          inDoubleQuote = false;
        }
      } else if (c == '\'') {
        inSingleQuote = true;
      } else if (c == '"') {
        inDoubleQuote = true;
      } else if (c == ':' && i + 1 < length
          && Character.isJavaIdentifierStart(query.charAt(i + 1))) {
        int j = i + 2;
        while (j < length && Character.isJavaIdentifierPart(query.charAt(j))) {
          j++;
        }
        String name = query.substring(i + 1, j);
        c = '?'; // replace the parameter with a question mark
        i += name.length(); // skip past the end if the parameter

        List indexList = (List) paramMap.get(name);
        if (indexList == null) {
          indexList = new LinkedList();
          paramMap.put(name, indexList);
        }
        indexList.add(new Integer(index));

        index++;
      }
      parsedQuery.append(c);
    }

    // replace the lists of Integer objects with arrays of ints
    for (Iterator itr = paramMap.entrySet().iterator(); itr.hasNext(); ) {
      Map.Entry entry = (Map.Entry) itr.next();
      List list = (List) entry.getValue();
      int[] indexes = new int[list.size()];
      int i = 0;
      for (Iterator itr2 = list.iterator(); itr2.hasNext(); ) {
        Integer x = (Integer) itr2.next();
        indexes[i++] = x.intValue();
      }
      entry.setValue(indexes);
    }

    return parsedQuery.toString();
  }

  /**
   * Returns the indexes for a parameter.
   *
   * @param name parameter name
   * @return parameter indexes
   * @throws IllegalArgumentException if the parameter does not exist
   */
  private int[] getIndexes(String name) {
    return (int[]) indexMap.get(name);
  }

  /**
   * Sets a parameter.
   *
   * @param name  parameter name
   * @param value parameter value
   * @throws SQLException             if an error occurred
   * @throws IllegalArgumentException if the parameter does not exist
   * @see PreparedStatement#setInt(int, int)
   */
  public void setLong(String name, long value) throws SQLException {
    int[] indexes = getIndexes(name);
    for (int i = 0; i < indexes.length; i++) {
      statement.setLong(indexes[i], value);
    }
  }

  /**
   * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value,
   * using the given <code>Calendar</code> object.  The driver uses
   * the <code>Calendar</code> object to construct an SQL <code>TIMESTAMP</code> value,
   * which the driver then sends to the database.  With a
   * <code>Calendar</code> object, the driver can calculate the timestamp
   * taking into account a custom timezone.  If no
   * <code>Calendar</code> object is specified, the driver uses the default
   * timezone, which is that of the virtual machine running the application.
   *
   * @param name the parameter name
   * @param x    the parameter value
   * @param cal  the <code>Calendar</code> object the driver will use
   *             to construct the timestamp
   * @throws SQLException if parameterIndex does not correspond to a parameter
   *                      marker in the SQL statement; if a database access error occurs or
   *                      this method is called on a closed <code>PreparedStatement</code>
   */
  public void setTimestamp(String name, Timestamp x, Calendar cal) throws SQLException {
    int[] indexes = getIndexes(name);
    for (int i = 0; i < indexes.length; i++) {
      statement.setTimestamp(indexes[i], x, cal);
    }
  }

  /**
   * Executes the statement, which must be a query.
   *
   * @return the query results
   * @throws SQLException if an error occurred
   * @see PreparedStatement#executeQuery()
   */
  public ResultSet executeQuery() throws SQLException {
    return statement.executeQuery();
  }

  /**
   * Closes the statement.
   *
   * @throws SQLException if an error occurred
   * @see java.sql.Statement#close()
   */
  public void close() throws SQLException {
    statement.close();
  }

  public Connection getConnection() throws SQLException {
    return statement.getConnection();
  }

}