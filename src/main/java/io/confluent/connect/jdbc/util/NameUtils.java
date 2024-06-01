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

public class NameUtils {

  private static final String TOPIC_PREFIX = "${topic";
  private static final String TOPIC_POSTFIX = "}";

  public static String toTableName(String rawName, String topic) {
    int topicStartPos = rawName.indexOf(TOPIC_PREFIX);
    int topicEndPos = rawName.lastIndexOf(TOPIC_POSTFIX);
    if (topicStartPos >= 0) {
      if (topicEndPos == topicStartPos + TOPIC_PREFIX.length()) {
        return rawName.replace(TOPIC_PREFIX + TOPIC_POSTFIX, topic);
      } else if (topicEndPos > topicStartPos + TOPIC_PREFIX.length()) {
        int subStartPos = topicStartPos + TOPIC_PREFIX.length() + 1;
        String splitter = rawName.substring(subStartPos - 1, subStartPos);
        int subMidPos = rawName.indexOf(splitter, subStartPos);
        int subEndPos = rawName.indexOf(splitter, subMidPos + 1);
        if (subMidPos > 0 && subEndPos > 0) {
          return rawName.substring(0, topicStartPos)
            + topic.replaceAll(rawName.substring(subStartPos, subMidPos),
                            rawName.substring(subMidPos + 1, subEndPos))
            + rawName.substring(topicEndPos + 1);
        }
      }
    }
    return rawName;
  }

}
