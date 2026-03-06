/**
 * Copyright [2025 - 2025] Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

import org.apache.kafka.common.Configurable;
import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import io.confluent.provider.integration.azure.AzureCredentialsProvider;

import java.util.Map;

/**
 * Azure Entra ID (formerly Azure Active Directory) credentials provider for JDBC connections.
 * <p>
 * This provider integrates with Azure's authentication system to obtain access tokens
 * that can be used for authenticating JDBC connections to Azure databases such as Azure SQL.
 * It leverages the {@link AzureCredentialsProvider} to handle token acquisition, caching,
 * and automatic refresh of expired tokens.
 * </p>
 * <p>
 * The provider must be configured with an Azure token scope that determines the resources
 * the token can access. Token management and caching are handled automatically by the
 * underlying Azure credentials provider.
 * </p>
 *
 * @see JdbcCredentialsProvider
 * @see AzureCredentialsProvider
 */
public class AzureJdbcCredentialsProvider  implements JdbcCredentialsProvider, Configurable {

  private AzureCredentialsProvider azureCredentialsProvider;
  private String tokenScope;

  /**
   * Configures this provider with the necessary Azure authentication settings.
   * <p>
   * This method initializes the Azure credentials provider and sets the token scope
   * required for accessing Azure resources. The token scope must be provided in the
   * configuration map under the key "azure.token.scope".
   * </p>
   *
   * @param config the configuration map containing Azure authentication settings.
   *               Must include "azure.token.scope" key with a non-empty value.
   * @throws IllegalArgumentException if the azure.token.scope is not provided or is empty
   */
  @Override
  public void configure(Map<String, ?> config) {
    this.tokenScope = (String) config.get("azure.token.scope");
    if (tokenScope == null || tokenScope.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Azure token scope must be provided in the configuration.");
    }

    this.azureCredentialsProvider = new AzureCredentialsProvider();
    azureCredentialsProvider.configure(config);
  }

  /**
   * Retrieves JDBC credentials using an Azure Entra ID access token.
   * <p>
   * This method synchronously obtains an access token from Azure using the configured
   * token scope. The token acquisition, caching, and automatic refresh are all handled
   * by the underlying {@link AzureCredentialsProvider}. The access token is returned
   * as the password credential, while the username is set to null as it is not required
   * for Azure token-based authentication.
   * </p>
   *
   * @return a {@link JdbcCredentials} object containing the Azure access token as the password
   * @throws RuntimeException if the Azure access token cannot be obtained or is null
   */
  @Override
  public JdbcCredentials getJdbcCredentials() {
    // AzureCredentialsProvider handles all token caching and refresh logic internally
    TokenRequestContext tokenRequestContext = new TokenRequestContext();
    tokenRequestContext.addScopes(tokenScope);
    AccessToken token = azureCredentialsProvider.getTokenSync(tokenRequestContext);

    if (token == null || token.getToken() == null) {
      throw new RuntimeException("Failed to obtain Azure Entra ID token");
    }

    return new BasicJdbcCredentials(null, token.getToken()); //username is not required
  }
}
