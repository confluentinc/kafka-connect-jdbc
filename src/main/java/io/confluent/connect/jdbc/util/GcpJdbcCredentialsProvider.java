/**
 * Copyright [2025 - 2025] Confluent Inc.
 */


package io.confluent.connect.jdbc.util;

import com.google.auth.oauth2.AccessToken;
import io.confluent.provider.integration.gcp.DelegatedImpersonatedGoogleCredentials;
import org.apache.kafka.common.Configurable;

import java.time.Instant;
import java.util.Map;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;

/**
 * Google Cloud Platform (GCP) credentials provider for JDBC connections to Cloud SQL.
 * <p>
 * This provider integrates with GCP's authentication system to obtain IAM access tokens
 * for authenticating JDBC connections to Cloud SQL databases. It uses delegated service
 * account impersonation through {@link DelegatedImpersonatedGoogleCredentials} to acquire
 * and manage access tokens.
 * </p>
 * <p>
 * The provider implements token caching and automatic refresh to minimize authentication
 * overhead. Tokens are cached and reused until they approach expiration (default: 55 minutes),
 * at which point a new token is automatically obtained.
 * </p>
 *
 * @see JdbcCredentialsProvider
 * @see DelegatedImpersonatedGoogleCredentials
 */
public class GcpJdbcCredentialsProvider implements JdbcCredentialsProvider, Configurable {

  private static final String TOKEN_REFRESH_SECONDS_CONFIG_KEY = "";
  // Default token validity is 60 minutes, but we refresh it earlier to avoid issues
  private static final long DEFAULT_TOKEN_REFRESH_SECONDS = 55 * 60; // 55 minutes

  private DelegatedImpersonatedGoogleCredentials delegatedImpersonatedGoogleCredentials;
  private String dbUser;
  private long tokenRefreshSeconds;

  // Cached credentials
  private String cachedAuthToken;
  private Instant nextTokenRefreshTime;

  /**
   * Configures this provider with the necessary GCP authentication settings.
   * <p>
   * This method initializes the GCP credentials provider and sets up token refresh
   * parameters. The database username must be provided in the configuration. Optionally,
   * a custom token refresh interval can be specified; otherwise, the default of 55 minutes
   * is used.
   * </p>
   *
   * @param config the configuration map containing GCP authentication settings.
   *               Must include the database username via CONNECTION_USER_CONFIG.
   *               May optionally include a custom token refresh interval.
   * @throws IllegalArgumentException if the database user is not provided
   */
  @Override
  public void configure(Map<String, ?> config) {
    this.dbUser = (String) config.get(CONNECTION_USER_CONFIG);

    if (dbUser == null) {
      throw new IllegalArgumentException(
          "Database user must be provided in the configuration.");
    }

    String tokenRefreshConfig = (String) config.get(TOKEN_REFRESH_SECONDS_CONFIG_KEY);
    this.tokenRefreshSeconds = tokenRefreshConfig != null
        ? Long.parseLong(tokenRefreshConfig)
        : DEFAULT_TOKEN_REFRESH_SECONDS;

    this.delegatedImpersonatedGoogleCredentials = new DelegatedImpersonatedGoogleCredentials();
    delegatedImpersonatedGoogleCredentials.configure(config);

    this.nextTokenRefreshTime = Instant.EPOCH; // force refresh initially
  }

  /**
   * Retrieves JDBC credentials using a GCP Cloud SQL IAM access token.
   * <p>
   * This method returns cached credentials if a valid token exists. If the cached token
   * is expired or does not exist, it obtains a new access token from GCP using the
   * configured delegated impersonated service account credentials. The new token is
   * cached along with its refresh time to avoid unnecessary authentication requests.
   * </p>
   *
   * @return a {@link JdbcCredentials} object containing the database username and
   *         GCP IAM access token as the password
   * @throws RuntimeException if the Cloud SQL IAM token cannot be obtained or is null
   */
  @Override
  public JdbcCredentials getJdbcCredentials() {
    if (isTokenValid()) {
      return new BasicJdbcCredentials(dbUser, cachedAuthToken);
    }

    AccessToken token = delegatedImpersonatedGoogleCredentials.refreshAccessToken();
    if (token == null || token.getTokenValue() == null) {
      throw new RuntimeException("Failed to obtain Cloud SQL IAM token");
    }

    this.cachedAuthToken = token.getTokenValue();
    this.nextTokenRefreshTime = Instant.now().plusSeconds(tokenRefreshSeconds);

    return new BasicJdbcCredentials(dbUser, cachedAuthToken);
  }

  /**
   * Checks if the cached authentication token is still valid.
   * <p>
   * A token is considered valid if it exists and the current time is before
   * the scheduled refresh time. This method is used internally to determine
   * whether to reuse the cached token or obtain a new one.
   * </p>
   *
   * @return true if a cached token exists and has not reached its refresh time,
   *         false otherwise
   */
  private boolean isTokenValid() {
    if (cachedAuthToken == null) {
      return false;
    }
    return Instant.now().isBefore(nextTokenRefreshTime);
  }
}
