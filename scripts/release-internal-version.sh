#!/bin/bash

set -euo pipefail

# No colors to avoid character encoding issues

# AWS CodeArtifact configuration
DOMAIN_OWNER="519856050701"
DOMAIN="confluent"
REGION="us-west-2"
REPOSITORY="maven"
NAMESPACE="io/confluent"
ARTIFACT_ID="kafka-connect-jdbc"

echo "Starting internal release for kafka-connect-jdbc"

# Extract base version from pom.xml (e.g., "10.8.5-SNAPSHOT" -> "10.8.5")
BASE_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout -B | sed 's/-SNAPSHOT//')
echo "Base version from pom.xml: ${BASE_VERSION}"

# Function to get AWS CodeArtifact authorization token
get_codeartifact_token() {
    echo "Getting AWS CodeArtifact authorization token..."
    aws codeartifact get-authorization-token \
        --domain-owner "$DOMAIN_OWNER" \
        --domain "$DOMAIN" \
        --region "$REGION" \
        --query authorizationToken \
        --output text
}

# Function to get existing versions from CodeArtifact
get_existing_versions() {
    local token=$1
    echo "Fetching existing versions from CodeArtifact..."
    
    # Get maven-metadata.xml
    local metadata_url="https://confluent-${DOMAIN_OWNER}.d.codeartifact.${REGION}.amazonaws.com/maven/${REPOSITORY}/${NAMESPACE}/${ARTIFACT_ID}/maven-metadata.xml"
    local auth_header="Authorization: Basic $(echo -n "aws:${token}" | base64)"
    
    # Fetch metadata and extract versions matching our base version pattern
    curl -s -H "$auth_header" "$metadata_url" | \
        grep -o "<version>${BASE_VERSION}_[0-9]\+</version>" | \
        sed 's/<version>\(.*\)<\/version>/\1/' || true
}

# Function to determine next version
determine_next_version() {
    local existing_versions=$1
    local max_suffix=0
    
    if [[ -z "$existing_versions" ]]; then
        echo "${BASE_VERSION}_1"
        return
    fi
    
    # Find the maximum suffix number
    while IFS= read -r version; do
        if [[ "$version" =~ ${BASE_VERSION}_([0-9]+) ]]; then
            local suffix=${BASH_REMATCH[1]}
            if (( suffix > max_suffix )); then
                max_suffix=$suffix
            fi
        fi
    done <<< "$existing_versions"
    
    local next_suffix=$((max_suffix + 1))
    echo "${BASE_VERSION}_${next_suffix}"
}

# Function to update pom.xml version
update_pom_version() {
    local new_version=$1
    echo "Updating pom.xml version to ${new_version}"
    
    # Use sed to replace the version in pom.xml
    sed -i.bak "s|<version>${BASE_VERSION}-SNAPSHOT</version>|<version>${new_version}</version>|" pom.xml
    
    # Verify the change
    if grep -q "<version>${new_version}</version>" pom.xml; then
        echo "Successfully updated pom.xml version to ${new_version}"
    else
        echo "Failed to update pom.xml version"
        exit 1
    fi
}

# Function to restore pom.xml version
restore_pom_version() {
    if [[ -f pom.xml.bak ]]; then
        echo "Restoring original pom.xml version"
        mv pom.xml.bak pom.xml
    fi
}

# Function to deploy to CodeArtifact
deploy_to_codeartifact() {
    local token=$1
    local version=$2
    
    echo "Deploying version ${version} to CodeArtifact..."
    
    # Configure maven settings for CodeArtifact
    local codeartifact_url="https://confluent-${DOMAIN_OWNER}.d.codeartifact.${REGION}.amazonaws.com/maven/${REPOSITORY}/"
    echo "Using CodeArtifact URL: ${codeartifact_url}"
    
    # Create temporary settings.xml with CodeArtifact configuration
    local temp_settings="/tmp/settings-codeartifact.xml"
    cat > "$temp_settings" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<settings>
    <servers>
        <server>
            <id>confluent-codeartifact-internal</id>
            <username>aws</username>
            <password>${token}</password>
        </server>
    </servers>
</settings>
EOF
    
    # Deploy both jar and test-jar
    echo "Starting Maven deployment..."
    mvn -s "$temp_settings" -Dcloud -Pjenkins -U \
        -Dmaven.wagon.http.retryHandler.count=10 \
        -Ddependency.check.skip=true \
        -B --batch-mode --no-transfer-progress \
        -DaltDeploymentRepository="confluent-codeartifact-internal::default::${codeartifact_url}" \
        -DrepositoryId=confluent-codeartifact-internal \
        clean deploy -DskipTests 2>&1
    
    local deploy_result=$?
    echo "Maven deployment finished with exit code: ${deploy_result}"
    
    # Clean up temporary settings file
    rm -f "$temp_settings"
    
    if [[ $deploy_result -eq 0 ]]; then
        echo "Successfully deployed version ${version} to CodeArtifact"
        echo "Artifacts deployed:"
        echo "  - ${ARTIFACT_ID}-${version}.jar"
        echo "  - ${ARTIFACT_ID}-${version}-test.jar"
    else
        echo "Failed to deploy to CodeArtifact"
        exit 1
    fi
}

# Main execution
main() {
    echo "=== Kafka Connect JDBC Internal Release ==="
    
    # Get CodeArtifact token
    TOKEN=$(get_codeartifact_token)
    if [[ -z "$TOKEN" ]]; then
        echo "Failed to get CodeArtifact authorization token"
        exit 1
    fi
    
    # Get existing versions
    EXISTING_VERSIONS=$(get_existing_versions "$TOKEN")
    echo "Existing versions matching ${BASE_VERSION}_x pattern:"
    if [[ -n "$EXISTING_VERSIONS" ]]; then
        echo "$EXISTING_VERSIONS"
    else
        echo "None found"
    fi
    
    # Determine next version
    NEXT_VERSION=$(determine_next_version "$EXISTING_VERSIONS")
    echo "Next version to deploy: ${NEXT_VERSION}"
    
    # Trap to ensure pom.xml is restored on exit
    trap restore_pom_version EXIT
    
    # Update pom.xml with new version
    update_pom_version "$NEXT_VERSION"
    
    # Deploy to CodeArtifact
    deploy_to_codeartifact "$TOKEN" "$NEXT_VERSION"
    
    echo "=== Internal release completed successfully ==="
    echo "Released version: ${NEXT_VERSION}"
    echo "Repository: https://confluent-${DOMAIN_OWNER}.d.codeartifact.${REGION}.amazonaws.com/maven/${REPOSITORY}/"
}

# Run main function
main "$@"
