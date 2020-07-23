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

package io.confluent.connect.jdbc;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class MysqlTestContainer extends GenericContainer<MysqlTestContainer> {

  private static final String DEFAULT_DOCKER_IMAGE = "mysql:5.7";
  private static final int MYSQL_DEFAULT_PORT = 3306;

  public MysqlTestContainer() {
    this(DEFAULT_DOCKER_IMAGE);
  }

  public MysqlTestContainer(String dockerImageName) {
    super(dockerImageName);
    this.logger().info("Starting an Mysql test container using [{}]", dockerImageName);
    withEnv("MYSQL_DATABASE", "db");
    withEnv("MYSQL_USER", "user");
    withEnv("MYSQL_PASSWORD", "password");
    withEnv("MYSQL_ROOT_PASSWORD", "password");
    withExposedPorts(MYSQL_DEFAULT_PORT);
    this.addFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock", BindMode.READ_WRITE);
    this.withCreateContainerCmdModifier(cmd -> cmd.withName("mysqlContainer"));
    this.setWaitStrategy(Wait.defaultWaitStrategy());
  }

  /**
   * Get the Elasticsearch connection URL.
   *
   * <p>This can only be called once the container is started.
   *
   * @return the connection URL; never null
   */
  public String getConnectionUrl() {
    String protocol = "http";
    return String.format(
        "%s://%s:%d",
        protocol,
        getContainerIpAddress(),
        getMappedPort(MYSQL_DEFAULT_PORT)
    );
  }

}
