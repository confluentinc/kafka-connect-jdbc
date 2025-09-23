#!/bin/bash

set -euo pipefail

# No colors to avoid character encoding issues

# AWS CodeArtifact configuration
DOMAIN_OWNER="519856050701"
DOMAIN="confluent"
REGION="us-west-2"
REPOSITORY="maven-snapshots"
NAMESPACE="io/confluent"
ARTIFACT_ID="kafka-connect-jdbc"

echo "Starting internal release for kafka-connect-jdbc"

# Extract version from pom.xml and determine release base version
POM_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout -B)
echo "POM version: ${POM_VERSION}"

# If version is X.Y.Z-SNAPSHOT, we should release internal versions based on X.Y.(Z-1)
if [[ "$POM_VERSION" == *-SNAPSHOT ]]; then
    SNAPSHOT_VERSION=$(echo "$POM_VERSION" | sed 's/-SNAPSHOT//')
    # Extract major.minor.patch and decrement patch version
    if [[ "$SNAPSHOT_VERSION" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        MAJOR=${BASH_REMATCH[1]}
        MINOR=${BASH_REMATCH[2]}
        PATCH=${BASH_REMATCH[3]}
        RELEASE_PATCH=$((PATCH - 1))
        BASE_VERSION="${MAJOR}.${MINOR}.${RELEASE_PATCH}"
    else
        echo "Error: Unable to parse version format: ${SNAPSHOT_VERSION}"
        exit 1
    fi
else
    # If not a snapshot, use the version as-is
    BASE_VERSION="$POM_VERSION"
fi

echo "Base version for internal release: ${BASE_VERSION}"

# Function to get AWS CodeArtifact authorization token
get_codeartifact_token() {
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

    # Get maven-metadata.xml
    local metadata_url="https://confluent-${DOMAIN_OWNER}.d.codeartifact.${REGION}.amazonaws.com/maven/${REPOSITORY}/${NAMESPACE}/${ARTIFACT_ID}/maven-metadata.xml"
    local auth_header="Authorization: Basic $(echo -n "aws:${token}" | base64)"

    # If POM version is not a snapshot, check if exact version exists
    if [[ "$POM_VERSION" != *-SNAPSHOT ]]; then
        curl -s -H "$auth_header" "$metadata_url" | \
            grep -o "<version>$POM_VERSION</version>" | \
            sed -E 's/<\/?version>//g' || true
    else
        # For snapshot versions, fetch versions matching our base version pattern with _x suffix
        curl -s -H "$auth_header" "$metadata_url" | \
                    grep -oE "<version>${BASE_VERSION//./\\.}[^<]*</version>" | \
                    sed -E 's/<\/?version>//g' || true
    fi
}

# Function to determine next version
determine_next_version() {
    local existing_versions=$1
    local max_suffix=0
    
    # If POM version is not a snapshot, release with the same version
    if [[ "$POM_VERSION" != *-SNAPSHOT ]]; then
        echo "$BASE_VERSION"
        return
    fi
    
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
    
    # Use Maven versions plugin to set the new version
    mvn versions:set -DnewVersion="${new_version}" -DgenerateBackupPoms=true --batch-mode --no-transfer-progress -q
    
    # Verify the change
    local current_version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout -B)
    if [[ "$current_version" == "$new_version" ]]; then
        echo "Successfully updated pom.xml version to ${new_version}"
    else
        echo "Failed to update pom.xml version. Expected: ${new_version}, Got: ${current_version}"
        exit 1
    fi
}

# Function to restore pom.xml version
restore_pom_version() {
    # Check if backup poms exist (created by versions:set)
    if [[ -f pom.xml.versionsBackup ]] || find . -name "*.versionsBackup" -type f | grep -q .; then
        echo "Restoring original pom.xml version"
        mvn versions:revert --batch-mode --no-transfer-progress -q
        echo "Successfully reverted pom.xml to original version"
    else
        echo "No backup poms found, nothing to restore"
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
    
    # Deploy both jar and test-jar
    echo "Starting Maven deployment..."
    mvn -Dcloud -Pjenkins -U \
        -Dmaven.wagon.http.retryHandler.count=10 \
        -Ddependency.check.skip=true \
        --batch-mode --no-transfer-progress \
        -DaltDeploymentRepository="confluent-codeartifact-internal::default::${codeartifact_url}" \
        -DrepositoryId=confluent-codeartifact-internal \
        clean deploy -DskipTests 2>&1
    
    local deploy_result=$?
    echo "Maven deployment finished with exit code: ${deploy_result}"
    
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
    echo "Existing versions fetched: $(echo "$EXISTING_VERSIONS" | tr '\n' ' ')"
    if [[ "$POM_VERSION" != *-SNAPSHOT ]]; then
        echo "Checking if version ${BASE_VERSION} already exists:"
        if [[ -n "$EXISTING_VERSIONS" ]]; then
            echo "Version ${BASE_VERSION} already exists in CodeArtifact"
            echo "Skipping deployment as version already exists"
            exit 0
        else
            echo "Version ${BASE_VERSION} not found, proceeding with deployment"
        fi
    else
        echo "Existing versions matching ${BASE_VERSION}_x pattern:"
        if [[ -n "$EXISTING_VERSIONS" ]]; then
            echo "$EXISTING_VERSIONS"
        else
            echo "None found"
        fi
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
