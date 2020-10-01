/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.jdbc;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class PumbaPauseContainer extends GenericContainer<PumbaPauseContainer> {

  private static final String DEFAULT_DOCKER_IMAGE = "gaiaadm/pumba:latest";

  private static final String PUMBA_PAUSE_COMMAND =
      "--log-level debug --interval 30s pause --duration 10s mysqlContainer";

  public PumbaPauseContainer() {
    this(DEFAULT_DOCKER_IMAGE);
  }

  public PumbaPauseContainer(String dockerImageName) {
    super(dockerImageName);
    this.logger().info("Starting an Pumba pause container using [{}]", dockerImageName);
    this.setCommand(PUMBA_PAUSE_COMMAND);
    this.addFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock", BindMode.READ_WRITE);
    this.withCreateContainerCmdModifier(cmd -> cmd.withName("pumba"));
    this.setWaitStrategy(Wait.defaultWaitStrategy());
  }
}
