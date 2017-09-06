/**
 * Copyright 2017 TouK.
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
package io.confluent.connect.jdbc.source;

import java.util.Map;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingJdbcSourceTask extends JdbcSourceTask {

  private static final Logger log = LoggerFactory.getLogger(LoggingJdbcSourceTask.class);

  @Override
  public void start(Map<String, String> properties) {
    super.start(properties);
    log.info("Starting jdbc task with properties: {}", properties);
    String connectorName = properties.get("name");
    tableQueue = wrapQueueContents(tableQueue, connectorName);
  }

  //we wrap all TableQueriers with our delegate to log each batch iterations
  private PriorityQueue<TableQuerier> wrapQueueContents(PriorityQueue<TableQuerier> original, String connectorName) {
    PriorityQueue<TableQuerier> toReturn = new PriorityQueue<>();
    for (TableQuerier querier : original) {
      String name = querier.name != null ? String.format("%s.%s", connectorName, querier.name) : connectorName;
      LoggingTableQuerier delegate = new LoggingTableQuerier(querier, name);
      toReturn.add(delegate);
    }
    return toReturn;
  }
}
