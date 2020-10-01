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

public class PumbaDelayContainer extends GenericContainer<PumbaDelayContainer> {

  private static final String DEFAULT_DOCKER_IMAGE = "gaiaadm/pumba:latest";

  private static final String PUMBA_PAUSE_COMMAND =
      "--log-level debug --interval 5s netem --tc-image gaiadocker/iproute2 "
          + "--duration 1s delay mysqlContainer";

  public PumbaDelayContainer() {
    this(DEFAULT_DOCKER_IMAGE);
  }

  public PumbaDelayContainer(String dockerImageName) {
    super(dockerImageName);
    this.logger().info("Starting an Pumba delay container using [{}]", dockerImageName);
    this.setCommand(PUMBA_PAUSE_COMMAND);
    this.addFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock", BindMode.READ_WRITE);
    this.withCreateContainerCmdModifier(cmd -> cmd.withName("pumba"));
    this.setWaitStrategy(Wait.defaultWaitStrategy());
  }
}
