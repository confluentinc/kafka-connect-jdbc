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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * General string utilities that are missing from the standard library and may commonly be
 * required by Connector or Task implementations.
 */
public class StringUtils {

  /**
   * Generate a String by appending all the @{elements}, converted to Strings, delimited by
   * @{delim}.
   * @param elements list of elements to concatenate
   * @param delim delimiter to place between each element
   * @return the concatenated string with delimiters
   */
  public static <T> String join(Iterable<T> elements, String delim) {
    StringBuilder result = new StringBuilder();
    boolean first = true;
    for (T elem : elements) {
      if (first) {
        first = false;
      } else {
        result.append(delim);
      }
      result.append(elem);
    }
    return result.toString();
  }
  
  //temporary (ugly) fix as getList(key) in AbstractConfig.java doesn't support escaped String (https://issues.apache.org/jira/browse/KAFKA-4524)
  public static List<String> getListFromStringValueWithEscapedCommas(String value,
      boolean removeCommaEscaping) {
    List<String> list = new ArrayList<String>();
    String[] parts = value.split("(?<!\\\\),");
    for (String s : parts) {
      String s2 = (removeCommaEscaping) ? s.replaceAll("\\\\", "") : s;
      list.add(s2);
    }
    return list;
  }
 
  public static String augmentCommaEscapingForTaskConfigParsing(String value) {
    return value.replaceAll("\\\\", "\\\\\\\\\\\\\\\\");
  }
 
  public static String removeCommaEscaping(String value) {
    return value.replaceAll("\\\\", "");
  }
  
  public static Map<String, String> listsToMap(List<String> keysList, List<String> valuesList) {
    if (keysList.size() != valuesList.size()) {
      throw new IllegalArgumentException("keysList and valuesList must have the same size.");
    }                                 
    Map<String, String> mapFromLists = new HashMap<>();
    int entryNumber = 0;
    for (String key :keysList) {
      mapFromLists.put(key, valuesList.get(entryNumber));
      entryNumber++;
    }
    return mapFromLists;
  }
}
