# Kafka Connect JDBC Version Selection Guide

This document explains how version numbers are selected and which version type you should use depending on your needs.

## Version Types

The Kafka Connect JDBC connector has two main types of versions:

### 1. **Released Versions (CP Versions)**
- **Format**: `X.Y.Z` (e.g., `10.8.4`, `10.8.3`)
- **Characteristics**: 
  - Clean version numbers without underscores or suffixes
  - No hyphens or special characters
- **Use Case**: **Production deployments and official releases**
- **Stability**: Highest - these are officially released, tested versions

### 2. **Incremental Versions**
- **Format**: `X.Y.Z_N` (e.g., `10.8.4_1`, `10.8.4_2`, `10.8.4_3`)
- **Characteristics**:
  - Based on the latest released version
  - Incremental suffix with underscore (`_1`, `_2`, etc.)
  - Generated automatically by the release script
- **Use Case**: **Internal testing, development, and CAN BE USED IN PRODUCTION by other projects**
- **Stability**: High - built from tested code, suitable for production use when teams are confident in their testing
- **Why Use**: Allows teams to get latest features/fixes without waiting for official CP releases that bundle 4 connectors (MySQL, PostgreSQL, Oracle, SQL Server)

## Version Selection Logic for the next artifact

The `release-internal-version.sh` script uses the following logic to determine the next version:

### For SNAPSHOT Versions (Development Branch)
1. **Find Base Version**: Query CodeArtifact for the latest released version (without `_` or `-`)
   ```
   # Query filters: !contains(version, '_') && !contains(version, 'SNAPSHOT') && !contains(version, '-')
   ```

2. **Check Existing incremental versions**: Look for existing versions with pattern `{BASE_VERSION}_x`
   ```
   # Example: If base is 10.8.4, look for 10.8.4_1, 10.8.4_2, etc.
   ```

3. **Generate Next Version**: Increment the highest existing suffix
   ```
   # If 10.8.4_2 exists, create 10.8.4_3
   # If none exist, create 10.8.4_1
   ```

### For CP Release
- Connector release job takes care of removing the `-SNAPSHOT` from the version and releasing the version to the same 
- artifactory. If `10.8.4_7` was the latest incremental version, then on next CP release `10.8.5` would be released.

## Which Version Should You Use?

### For Production Use - Option 1 (Most Stable)
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

### For Production Use - Option 2 (Faster Updates)
```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-connect-jdbc</artifactId>
    <version>10.8.4_3</version> <!-- Latest incremental version -->
</dependency>
```
**✅ Use**: Latest incremental version with underscore suffix
**✅ Best for**: Teams that need latest features/fixes and have confidence in their testing
**✅ Why**: Avoid waiting for CP releases that bundle multiple connectors
**⚠️ Requirement**: Team must be comfortable with their own testing and validation

### For Internal Testing/Development
```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-connect-jdbc</artifactId>
    <version>10.8.4_3</version> <!-- Latest incremental version -->
</dependency>
```
**✅ Use**: Latest incremental version with underscore suffix

### For Local Development
- **Build from source** using the current development version in `pom.xml`
- **No released artifacts** for development versions

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

```
CP Release Cycle:
10.8.3 ──────────────────────► 10.8.4 ──────────────────────► 10.8.5 ──────────────────────► 10.8.6
  │                               │                               │                               │
  │ Incremental Versions          │ Incremental Versions          │ Incremental Versions          │ Incremental Versions
  ├─ 10.8.3_1                     ├─ 10.8.4_1                     ├─ 10.8.5_1                     ├─ 10.8.6_1
  ├─ 10.8.3_2                     ├─ 10.8.4_2                     ├─ 10.8.5_2                     ├─ 10.8.6_2
  ├─ 10.8.3_3                     ├─ 10.8.4_3                     ├─ 10.8.5_3                     ├─ 10.8.6_3
  └─ ...                          ├─ 10.8.4_4                     ├─ 10.8.5_4                     └─ ...
                                  ├─ 10.8.4_5                     └─ ...
                                  ├─ 10.8.4_6
                                  └─ 10.8.4_7 (latest before 10.8.5)
```

### Current State Example:
If the **current latest CP version is 10.8.4**, then:

1. **Development work** happens locally by building from source
2. **Incremental releases** are created: `10.8.4_1`, `10.8.4_2`, `10.8.4_3`, etc.
3. **Production deployment options**:
   - Use `10.8.4` (stable CP version)
   - Use `10.8.4_7` (latest incremental with newest features)
4. **Next CP release** will be `10.8.5`
5. **After 10.8.5 release**, new incremental versions become: `10.8.5_1`, `10.8.5_2`, etc.

### Version Flow:
```
Development (Local) → Incremental Release → Production Choice → Next CP Release
       ↓                      ↓                    ↓                ↓
Build from source → 10.8.4_1, 10.8.4_2... → Use 10.8.4 or 10.8.4_x → 10.8.5 released
                                                                        ↓
                                              New cycle starts → 10.8.5_1, 10.8.5_2...
```
