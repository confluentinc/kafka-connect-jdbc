/*
 * 
 */

package io.confluent.connect.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.AWS_SECRET_ACCESS_KEY_CONFIG;


public class SNSClient {
  private final Logger log = LoggerFactory.getLogger(SNSClient.class);


  public static final Class<? extends AWSCredentialsProvider> CREDENTIALS_PROVIDER_CLASS_DEFAULT =
      com.amazonaws.auth.DefaultAWSCredentialsProviderChain.class;

  private final AmazonSNS client;

  public SNSClient(JdbcSourceConnectorConfig config) {
    AWSCredentialsProvider provider = null;
    try {
      provider = newCredentialsProvider(config);
    } catch (Exception e) {
      log.error("Problem initializing provider", e);
    }
    final AmazonSNSClientBuilder builder = AmazonSNSClientBuilder.standard();
    builder.setCredentials(provider);
    client = builder.build();
  }


  /**
   * Publish a message to a topic.
   *
   * @param topicArn       SNS topic ARN.
   * @param body      The message to send.
   * @return
   */
  public void publish(String topicArn, String message) {
    final PublishRequest publishRequest = new PublishRequest(topicArn, message);
    final PublishResult publishResponse = client.publish(publishRequest);
    log.info("Message published to SNS topic with message id: {}", publishResponse.getMessageId());
  }


  protected AWSCredentialsProvider newCredentialsProvider(JdbcSourceConnectorConfig config) { 
    final String accessKeyId = config.getString(AWS_ACCESS_KEY_ID_CONFIG);
    final String secretKey = config.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value();
    if (!accessKeyId.isEmpty() && !secretKey.isEmpty()) {
      log.info("Returning new credentials provider using the access key id and "
          + "the secret access key that were directly supplied through the connector's "
          + "configuration");
      BasicAWSCredentials basicCredentials = new BasicAWSCredentials(accessKeyId, secretKey);
      return new AWSStaticCredentialsProvider(basicCredentials);
    }
    log.info(
        "Returning new credentials provider based on the configured credentials provider class");
    return config.getCredentialsProvider();
  }
  
  public Class<?> getClass(String className) {
    log.warn(".get-class:class={}",className);
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      log.error("Provider class not found: {}", e);
    }
    return null;
  }

}