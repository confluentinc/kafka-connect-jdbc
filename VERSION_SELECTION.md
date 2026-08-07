# Kafka Connect JDBC Version Selection Guide

This document explains how version numbers are selected and which version type you should use depending on your needs.

## Version Types

The Kafka Connect JDBC connector has two main types of versions:

### 1. **Released Versions (CP Versions)**
- **Format**: `X.Y.Z` (e.g., `10.9.6`, `10.9.5`)
- **Characteristics**:
  - Clean version numbers with no suffix after the patch number
  - Released through the official CP release pipeline (the `connect-releases` job)
  - Published to **packages.confluent.io**, the public Maven repository, and to Confluent Hub
- **Use Case**: **Production deployments and official releases**

### 2. **Incremental Versions**
- **Format**: `X.Y.Z-N` (e.g., `10.9.7-1`, `10.9.7-2`, `10.9.7-3`)
- **Characteristics**:
  - Based on the **upcoming** version — the branch's current development version with `-SNAPSHOT` stripped. On `10.9.x` with a pom version of `10.9.7-SNAPSHOT`, incrementals are `10.9.7-1`, `10.9.7-2`, and so on.
  - **Released via Semaphore pipeline promotion** after merging to the release branch
  - Version number is **automatically determined** from git tags by `ci-update-version`
  - Each release also pushes a matching git tag (e.g. `v10.9.7-1`)
  - **Requires manual promotion trigger** (not part of the automated pipeline flow)
  - Published **only to internal CodeArtifact** — never to packages.confluent.io
- **Purpose**: To unblock developers who need specific fixes or features in the framework without waiting for the next official CP release
- **Responsibility**: Since kafka-connect-jdbc is a common dependency used by many connectors, **developers must take full responsibility to thoroughly test their changes before triggering the promotion**
- **Use Case**: Production use by teams that have tested and are confident in the incremental version

## Which Version Should You Use?

### For Production Use - CP Versions
```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-connect-jdbc</artifactId>
    <version>10.9.6</version> <!-- Latest released CP version -->
</dependency>
```
**✅ Use**: Latest released version with no suffix (CP version)
**✅ Best for**: Teams that can wait for official CP releases
**⚠️ Note**: CP releases bundle 4 connectors and may have longer release cycles
**⚠️ Note**: Not every released version reaches packages.confluent.io promptly. If a release tag exists but the artifact 404s there, it may only be resolvable from an internal repository — check before pinning.

### For Production Use - Incremental Versions
```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-connect-jdbc</artifactId>
    <version>10.9.7-1</version> <!-- Latest incremental version -->
</dependency>
```
Incremental versions are not on packages.confluent.io, so your build must also declare the internal repository:
```xml
<repositories>
    <repository>
        <id>confluent-codeartifact-maven-snapshots</id>
        <url>https://confluent-519856050701.d.codeartifact.us-west-2.amazonaws.com/maven/maven-snapshots/</url>
    </repository>
</repositories>
```
**✅ Use**: Latest incremental version with a `-N` suffix
**✅ Best for**: Teams that need latest features/fixes and cannot wait for CP releases
**⚠️ Responsibility**: Developers must thoroughly test before triggering the pipeline promotion since kafka-connect-jdbc is a common dependency for many connectors
**⚠️ Always pin an exact version.** Maven orders `10.9.7-1` *after* `10.9.7`, so a version range or "latest" resolution can select an incremental in preference to a newer official release.

### For Local Development
- **Build from source** using the current development version in `pom.xml`

## Releasing an Incremental Version

### Pipeline Promotion Process
When a developer needs to release an incremental version to unblock themselves or their team:

1. **Ensure Thorough Testing**: Since kafka-connect-jdbc is a common dependency for many connectors, the developer must thoroughly test all changes

2. **Merge to the Release Branch**: Merge your tested changes to the release branch (e.g. `10.9.x`)

3. **Semaphore Pipeline Execution**: After the merge, the Semaphore pipeline will automatically run and complete

4. **Trigger Pipeline Promotion**: In the Semaphore pipeline interface, use the "Release incremental f/w version" promotion to:
   - Run `ci-tools ci-update-version`, which:
     - Reads the current development version from `pom.xml` (e.g. `10.9.7-SNAPSHOT` → `10.9.7`)
     - Lists git tags matching `v10.9.7-[0-9]*` and takes the highest build number
     - Sets the pom to the next version (e.g. `10.9.7-3`) and creates tag `v10.9.7-3`
   - Run `ci-tools ci-push-tag` to push that tag
   - Deploy the release automatically to CodeArtifact

**Important**: Triggering the pipeline promotion is the developer's commitment that they have thoroughly tested their changes and take full responsibility for the release.

**Note on the git history**: `ci-update-version` makes a local commit for the version bump and tags it. Only the tag is pushed — never a branch — so the tag deliberately points at a commit that is not in branch history.

## Finding Available Versions

### Latest Released Version (CP Version)
Released versions are published externally. The authoritative list is:

```bash
curl -s https://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/maven-metadata.xml
```

Git tags are a reliable cross-check — every CP release has a `vX.Y.Z` tag:

```bash
git tag -l 'v10.9.*' | sort -V
```

### Latest Incremental Version
Incremental versions live only in internal CodeArtifact. Identify the branch's development version from `pom.xml`, then look for the highest `-N` suffix on it.

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

# Filter for incremental versions of a given base (e.g. 10.9.7)
aws codeartifact list-package-versions \
  --domain confluent \
  --domain-owner 519856050701 \
  --repository maven-snapshots \
  --format maven \
  --namespace io.confluent \
  --package kafka-connect-jdbc \
  --region us-west-2 \
  --query "versions[?starts_with(version, '10.9.7-')].version" \
  --output text | tr '\t' '\n' | grep -E '^10\.9\.7-[0-9]+$' | sort -V
```

**Why the `grep` is needed**: this repository also contains timestamped snapshot builds such as `10.9.7-20260807.053902-8`, produced by the ordinary per-merge pipeline. They also start with `10.9.7-` and do not contain the string `SNAPSHOT`, so no CodeArtifact-side filter separates them. Incremental releases are the ones whose suffix is a plain integer.

## Version Lifecycle

### Overview
Incremental versions are based on the version currently under development on the branch, so the base advances when a CP release ships:

```
10.9.x branch

  v10.9.6 released  ──►  pom becomes 10.9.7-SNAPSHOT
                              │
                              ├─ 10.9.7-1   (promotion)
                              ├─ 10.9.7-2   (promotion)
                              └─ 10.9.7-3   (promotion)

  v10.9.7 released  ──►  pom becomes 10.9.8-SNAPSHOT
                              │
                              ├─ 10.9.8-1   (promotion)
                              └─ ...
```

**Note**: Incremental versions are released through Semaphore pipeline promotion. The version number is calculated automatically from git tags.

### Example Workflow:
If the **latest CP release is 10.9.6** and the branch pom is `10.9.7-SNAPSHOT`:

1. Developer works locally by building from source
2. Developer needs to unblock their team with a critical fix
3. Developer thoroughly tests their changes
4. Developer merges to the release branch
5. Semaphore pipeline runs and completes successfully
6. Developer triggers the "Release incremental f/w version" promotion → `10.9.7-1` is created, tagged `v10.9.7-1`, and deployed to internal CodeArtifact
7. Other teams can now use:
   - `10.9.6` (latest CP version, from packages.confluent.io)
   - `10.9.7-1` (with the critical fix, from internal CodeArtifact)
8. Next official CP release will be `10.9.7`
9. Once `10.9.7` is released the pom moves to `10.9.8-SNAPSHOT`, and future incremental versions will be `10.9.8-1`, `10.9.8-2`, etc.
