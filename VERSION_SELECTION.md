# Kafka Connect JDBC Version Selection Guide

This document explains how version numbers are selected and which version type you should use depending on your needs.

## Version Types

The Kafka Connect JDBC connector has two main types of versions:

### 1. **Released Versions (CP Versions)**
- **Format**: `X.Y.Z` (e.g., `10.8.4`, `10.8.3`)
- **Characteristics**: 
  - Clean version numbers without underscores or suffixes
  - No hyphens or special characters
  - Released through the official CP release pipeline
- **Use Case**: **Production deployments and official releases**

### 2. **Incremental Versions**
- **Format**: `X.Y.Z_N` (e.g., `10.8.4_1`, `10.8.4_2`, `10.8.4_3`)
- **Characteristics**:
  - Based on the latest released version
  - Incremental suffix with underscore (`_1`, `_2`, etc.)
  - **Released via Semaphore pipeline promotion** after merging to the feature branch
  - Version number is **automatically determined** by the `release-internal-version.sh` script
  - **Requires manual promotion trigger** (not part of the automated pipeline flow)
- **Purpose**: To unblock developers who need specific fixes or features in the framework without waiting for the next official CP release
- **Responsibility**: Since kafka-connect-jdbc is a common dependency used by many connectors, **developers must take full responsibility to thoroughly test their changes before triggering the promotion**
- **Use Case**: Production use by teams that have tested and are confident in the incremental version

## Which Version Should You Use?

### For Production Use - CP Versions
```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-connect-jdbc</artifactId>
    <version>10.8.4</version> <!-- Latest released CP version -->
</dependency>
```
**✅ Use**: Latest released version without underscores (CP version)
**✅ Best for**: Teams that can wait for official CP releases
**⚠️ Note**: CP releases bundle 4 connectors and may have longer release cycles

### For Production Use - Incremental Versions
```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-connect-jdbc</artifactId>
    <version>10.8.4_3</version> <!-- Latest incremental version -->
</dependency>
```
**✅ Use**: Latest incremental version with underscore suffix
**✅ Best for**: Teams that need latest features/fixes and cannot wait for CP releases
**⚠️ Responsibility**: Developers must thoroughly test before triggering the pipeline promotion since kafka-connect-jdbc is a common dependency for many connectors

### For Local Development
- **Build from source** using the current development version in `pom.xml`

## Releasing an Incremental Version

### Pipeline Promotion Process
When a developer needs to release an incremental version to unblock themselves or their team:

1. **Ensure Thorough Testing**: Since kafka-connect-jdbc is a common dependency for many connectors, the developer must thoroughly test all changes

2. **Merge to Feature Branch**: Merge your tested changes to the feature branch

3. **Semaphore Pipeline Execution**: After the merge, the Semaphore pipeline will automatically run and complete

4. **Trigger Pipeline Promotion**: In the Semaphore pipeline interface, use the "Release incremental f/w version" promotion to:
   - Automatically invoke the `release-internal-version.sh` script
   - Calculate the next version number based on existing versions:
     - Finds the latest CP version (e.g., `10.8.4`)
     - Checks for existing incremental versions (e.g., `10.8.4_1`, `10.8.4_2`)
     - Determines the next incremental number (e.g., `10.8.4_3`)
   - Deploy the release automatically to CodeArtifact

**Important**: Triggering the pipeline promotion is the developer's commitment that they have thoroughly tested their changes and take full responsibility for the release.

## Finding Available Versions

### Latest Released Version (CP Version)
To find the latest released version, look for versions that:
- Have no underscores (`_`)
- Have no hyphens (`-`)
- Follow semantic versioning (X.Y.Z)

### Latest Incremental Version
To find the latest incremental version:
1. Identify the latest released version (e.g., `10.8.4`)
2. Look for the highest numbered suffix (e.g., `10.8.4_5`)

### Using AWS CLI (Internal)
```bash
# List all versions
aws codeartifact list-package-versions \
  --domain confluent \
  --domain-owner 519856050701 \
  --repository maven-snapshots \
  --format maven \
  --namespace io.confluent \
  --package kafka-connect-jdbc \
  --region us-west-2

# Filter for incremental versions only
aws codeartifact list-package-versions \
  --domain confluent \
  --domain-owner 519856050701 \
  --repository maven-snapshots \
  --format maven \
  --namespace io.confluent \
  --package kafka-connect-jdbc \
  --region us-west-2 \
  --query "versions[?starts_with(version, '10.8.4_')].version" \
  --output text
```

## Version Lifecycle

### Overview
```
CP Release Cycle:
10.8.3 ────────► 10.8.4 ────────► 10.8.5 ────────► 10.8.6
  │                │                 │                │
  │ (Pipeline      │ (Pipeline       │ (Pipeline      │ (Pipeline
  │  Promotion)    │  Promotion)     │  Promotion)    │  Promotion)
  │                │                 │                │
  ├─ 10.8.3_1      ├─ 10.8.4_1       ├─ 10.8.5_1      ├─ 10.8.6_1
  └─ 10.8.3_2      ├─ 10.8.4_2       └─ 10.8.5_2      └─ ...
                   └─ 10.8.4_3
```

**Note**: Incremental versions are released through Semaphore pipeline promotion. The version number is automatically calculated by the release script.

### Example Workflow:
If the **current latest CP version is 10.8.4**:

1. Developer works locally by building from source
2. Developer needs to unblock their team with a critical fix
3. Developer thoroughly tests their changes
4. Developer merges to the feature branch
5. Semaphore pipeline runs and completes successfully
6. Developer triggers the "Release incremental f/w version" promotion → `10.8.4_1` is automatically created and deployed
7. Other teams can now use:
   - `10.8.4` (latest CP version)
   - `10.8.4_1` (with the critical fix)
8. Next official CP release will be `10.8.5`
9. After `10.8.5` is released, future incremental versions will be `10.8.5_1`, `10.8.5_2`, etc.
