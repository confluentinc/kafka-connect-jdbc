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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import com.google.re2j.Pattern;

/**
 * Utility class for filtering collections of tables using regular expression patterns.
 * This class provides sophisticated table filtering capabilities using include and exclude
 * regex patterns, which is more powerful than simple exact string matching.
 */
public class TableCollectionUtils {

  private TableCollectionUtils() {
    // Private constructor to prevent instantiation
  }

  /**
   * Filter tables using include and exclude regex patterns.
   * 
   * <p>The filtering process works as follows:
   * <ol>
   *   <li>Apply inclusion patterns first - only tables matching at least one inclusion 
   *       pattern are kept</li>
   *   <li>Apply exclusion patterns to the included tables - tables matching any exclusion 
   *       pattern are removed</li>
   *   <li>Remove duplicates while preserving order</li>
   * </ol>
   * 
   * <p>If the inclusion regex set is empty, no tables will be included regardless of 
   * exclusion patterns. If the exclusion regex set is empty, no tables will be excluded 
   * from the included set.
   * 
   * @param tables All available tables from the database
   * @param inclusionRegex Set of regex patterns for inclusion (empty means include none)
   * @param exclusionRegex Set of regex patterns for exclusion (empty means exclude none)
   * @return Filtered and deduplicated list of tables, preserving original order
   */
  public static List<TableId> filterTables(
      List<TableId> tables,
      Set<String> inclusionRegex,
      Set<String> exclusionRegex
  ) {
    List<TableId> filteredTables = new ArrayList<>();
    
    for (TableId table : tables) {
      String tableString = table.toUnquotedString();
      boolean includeMatch = matchesAny(tableString, inclusionRegex);
      boolean excludeMatch = matchesAny(tableString, exclusionRegex);
      
      // Use System.out for immediate visibility in logs
      System.out.println("DEBUG: Table " + tableString + " - Include match: " + includeMatch 
                        + ", Exclude match: " + excludeMatch + ", Final result: " 
                        + (includeMatch && !excludeMatch));
      
      if (includeMatch && !excludeMatch) {
        filteredTables.add(table);
      }
    }
    
    // Remove duplicates while preserving order using LinkedHashSet
    return new ArrayList<>(new LinkedHashSet<>(filteredTables));
  }

  /**
   * Validate that each table matches exactly one regex pattern from the provided list.
   * This is useful for validating column mapping configurations where each table should
   * match exactly one mapping rule.
   * 
   * @param regexes List of regex patterns to validate against
   * @param tableIds List of table identifiers to validate
   * @param tableIdToString Function to convert table ID to string for matching
   * @param problemReporter Consumer to report validation problems
   * @param <T> Type of table identifier
   * @return true if validation passes, false if any table matches zero or more than one
   */
  public static <T> boolean validateEachTableMatchesExactlyOneRegex(
      List<String> regexes,
      List<T> tableIds,
      Function<T, String> tableIdToString,
      Consumer<String> problemReporter
  ) {
    for (T tableId : tableIds) {
      int matchCount = 0;
      for (String regex : regexes) {
        if (Pattern.matches(regex, tableIdToString.apply(tableId))) {
          matchCount++;
        }
      }
      if (matchCount == 0) {
        problemReporter.accept("Included table '" + tableId.toString()
            + "' does not match any of the provided regexes");
        return false;
      } else if (matchCount > 1) {
        problemReporter.accept("Included table '" + tableId.toString()
            + "' matches more than one of the provided regexes");
        return false;
      }
    }
    return true;
  }

  /**
   * Check if a table string matches any of the provided regex patterns.
   * 
   * @param tableString The table identifier as a string
   * @param regexSet Set of regex patterns to match against
   * @return true if the table string matches at least one regex pattern, false otherwise
   */
  private static boolean matchesAny(String tableString, Set<String> regexSet) {
    if (regexSet == null || regexSet.isEmpty()) {
      return false; // Empty regex set means match nothing
    }
    
    for (String regex : regexSet) {
      if (Pattern.matches(regex, tableString)) {
        return true;
      }
    }
    return false;
  }
}
