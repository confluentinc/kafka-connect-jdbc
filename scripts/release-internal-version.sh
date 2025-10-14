#!/bin/bash

set -uo pipefail

# No colors to avoid character encoding issues

# AWS CodeArtifact configuration
DOMAIN_OWNER="519856050701"
DOMAIN="confluent"
REGION="us-west-2"
REPOSITORY="maven-snapshots"
NAMESPACE="io/confluent"
ARTIFACT_ID="kafka-connect-jdbc"

echo "Starting internal release for kafka-connect-jdbc"
echo ""

# Extract version from pom.xml and determine release base version
POM_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout -B)
pom_eval_result=$?
if [[ $pom_eval_result -ne 0 ]] || [[ -z "$POM_VERSION" ]]; then
    echo "Failed to extract version from pom.xml (exit code: $pom_eval_result)"
    echo "Please ensure Maven is properly configured and pom.xml is valid"
    exit 1
fi
echo "POM version: ${POM_VERSION}"

# Function to find the latest released version for internal releases
find_latest_released_version() {
    echo "Finding latest released version from CodeArtifact..." >&2
    
    # Get all released versions (non-snapshot, non-internal) from CodeArtifact
    local all_versions
    all_versions=$(aws codeartifact list-package-versions \
        --domain "$DOMAIN" \
        --domain-owner "$DOMAIN_OWNER" \
        --repository "$REPOSITORY" \
        --format maven \
        --namespace "${NAMESPACE//\//.}" \
        --package "$ARTIFACT_ID" \
        --region "$REGION" \
        --query "versions[?!contains(version, '_') && !contains(version, 'SNAPSHOT') && !contains(version, '-')].version" \
        --output text 2>/dev/null)
    
    local aws_exit_code=$?
    if [[ $aws_exit_code -ne 0 ]]; then
        echo "Warning: Could not fetch released versions from CodeArtifact (exit code: $aws_exit_code)" >&2
        return 1
    fi
    
    if [[ -z "$all_versions" || "$all_versions" == "None" ]]; then
        echo "No released versions found in CodeArtifact" >&2
        return 1
    fi
    
    # Sort versions and get the latest one, ensure we only return a single clean version
    local latest_version
    latest_version=$(echo "$all_versions" | tr '\t' '\n' | grep -v '^$' | sort -V | tail -1 | tr -d '\n\r')
    
    if [[ -n "$latest_version" ]]; then
        echo "$latest_version"
        return 0
    else
        echo "Could not determine latest version from response" >&2
        return 1
    fi
}

# Determine base version for internal releases
if [[ "$POM_VERSION" == *-SNAPSHOT ]]; then
    # For snapshot versions, find the latest released version to use as base
    LATEST_RELEASED=$(find_latest_released_version)
    if [[ $? -eq 0 && -n "$LATEST_RELEASED" ]]; then
        BASE_VERSION="$LATEST_RELEASED"
        echo "Using latest released version as base: ${BASE_VERSION}"
    else
        echo "Error: Cannot determine base version without CodeArtifact access"
        echo "Please configure AWS credentials to query the latest released version"
        echo "Run: aws configure"
        exit 1
    fi
else
    # If not a snapshot, it's a release version - use it as-is
    BASE_VERSION="$POM_VERSION"
    echo "Using release version as-is: ${BASE_VERSION}"
fi

echo "Base version for internal release: ${BASE_VERSION}"


# Function to get existing versions from CodeArtifact using AWS CLI
get_existing_versions() {
    # Only check for existing versions if POM version is a snapshot
    # For release versions, we'll just try to deploy and let Maven handle conflicts
    if [[ "$POM_VERSION" == *-SNAPSHOT ]]; then
        # For snapshot versions, fetch versions matching our base version pattern with _x suffix
        aws codeartifact list-package-versions \
            --domain "$DOMAIN" \
            --domain-owner "$DOMAIN_OWNER" \
            --repository "$REPOSITORY" \
            --format maven \
            --namespace "${NAMESPACE//\//.}" \
            --package "$ARTIFACT_ID" \
            --region "$REGION" \
            --query "versions[?starts_with(version, '${BASE_VERSION}_')].version" \
            --output text 2>&1
        
        local aws_exit_code=$?
        
        # Check if AWS CLI command failed
        if [[ $aws_exit_code -ne 0 ]]; then
            echo "Error: Failed to list package versions from CodeArtifact (exit code: $aws_exit_code)" >&2
            echo "Please check your AWS credentials and CodeArtifact permissions" >&2
            return 1
        fi
    else
        # For release versions, return empty (no existing versions to check)
        echo ""
    fi
    
    return 0
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
    # Convert tabs to newlines and process each version
    local versions_list
    versions_list=$(echo "$existing_versions" | tr '\t' '\n')
    
    while IFS= read -r version; do
        # Skip empty lines
        [[ -z "$version" ]] && continue
        
        if [[ "$version" =~ ${BASE_VERSION}_([0-9]+) ]]; then
            local suffix=${BASH_REMATCH[1]}
            if (( suffix > max_suffix )); then
                max_suffix=$suffix
            fi
        fi
    done <<< "$versions_list"
    
    local next_suffix=$((max_suffix + 1))
    echo "${BASE_VERSION}_${next_suffix}"
}

# Function to update pom.xml version
update_pom_version() {
    local new_version=$1
    echo "Updating pom.xml version to ${new_version}"
    
    # Use Maven versions plugin to set the new version
    mvn versions:set -DnewVersion="${new_version}" -DgenerateBackupPoms=true --batch-mode --no-transfer-progress -q
    local mvn_set_result=$?
    if [[ $mvn_set_result -ne 0 ]]; then
        echo "Failed to update pom.xml version using Maven versions plugin (exit code: $mvn_set_result)"
        exit 1
    fi
    
    # Verify the change
    current_version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout -B)
    local mvn_eval_result=$?
    if [[ $mvn_eval_result -ne 0 ]]; then
        echo "Failed to verify pom.xml version using Maven (exit code: $mvn_eval_result)"
        exit 1
    fi
    
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
    local version=$1
    
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
    echo "=== Kafka Connect JDBC artifact incremental release ==="

    # Get existing versions
    EXISTING_VERSIONS=$(get_existing_versions)
    local get_versions_result=$?
    if [[ $get_versions_result -ne 0 ]]; then
        echo "Failed to fetch existing versions from CodeArtifact"
        exit 1
    fi
    if [[ "$POM_VERSION" == *-SNAPSHOT ]]; then
        echo "Existing versions matching ${BASE_VERSION}_x pattern: $(echo "$EXISTING_VERSIONS")"
        if [[ -n "$EXISTING_VERSIONS" ]]; then
            echo "$EXISTING_VERSIONS"
        else
            echo "None found"
        fi
    else
        echo "Release version deployment - will attempt to deploy ${BASE_VERSION} directly"
    fi
    
    # Determine next version
    NEXT_VERSION=$(determine_next_version "$EXISTING_VERSIONS")
    echo "Next version to deploy: ${NEXT_VERSION}"
    
    # Trap to ensure pom.xml is restored on exit
    trap restore_pom_version EXIT
    
    # Update pom.xml with new version
    update_pom_version "$NEXT_VERSION"
    
    # Deploy to CodeArtifact
    deploy_to_codeartifact "$NEXT_VERSION"
    
    echo "=== Internal release completed successfully ==="
    echo "Released version: ${NEXT_VERSION}"
    echo "Repository: https://confluent-${DOMAIN_OWNER}.d.codeartifact.${REGION}.amazonaws.com/maven/${REPOSITORY}/"
}

# Run main function
main "$@"
