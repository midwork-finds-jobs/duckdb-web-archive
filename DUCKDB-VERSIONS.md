# DuckDB Version Strategy

This extension uses a branch-based versioning strategy to automatically support new DuckDB patch releases.

## Problem

Previously, the CI workflow hardcoded exact DuckDB versions:

```yaml
# Old approach - breaks when 1.4.3 releases
uses: duckdb/extension-ci-tools/...@v1.4.2
with:
  duckdb_version: v1.4.2
  ci_tools_version: v1.4.2
```

When DuckDB released 1.4.3, 1.4.4, etc., the extension wouldn't automatically build against them.

## Solution

DuckDB uses **version branches** with duck codenames that track all patch releases:

- `v1.4-andium` → contains all 1.4.x releases (1.4.0, 1.4.1, 1.4.2, 1.4.3, ...)
- `v1.5-variegata` → contains all 1.5.x releases
- `main` → development/nightly

By tracking these version branches instead of exact tags, the extension automatically builds against new patch releases.

## Branch Strategy

| Branch | duckdb_version | ci_tools_version | Purpose |
|--------|----------------|------------------|---------|
| main | main | main | Track DuckDB development, early testing |
| v1.4 | v1.4-andium | main | Stable 1.4.x releases |
| v1.5 | v1.5-variegata | main | Stable 1.5.x releases |

### Workflow Configuration

Each branch has `MainDistributionPipeline.yml` configured to track its DuckDB version:

**main branch:**

```yaml
uses: duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@main
with:
  duckdb_version: main
  ci_tools_version: main
```

**v1.4 branch:**

```yaml
uses: duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@main
with:
  duckdb_version: v1.4-andium
  ci_tools_version: main
```

**v1.5 branch:**

```yaml
uses: duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@main
with:
  duckdb_version: v1.5-variegata
  ci_tools_version: main
```

## Scheduled Builds

Daily scheduled workflows trigger builds on stable branches to catch compatibility issues early:

- `scheduled-1.4.yml` - triggers v1.4 branch build daily at 12:00 UTC
- `scheduled-1.5.yml` - triggers v1.5 branch build daily at 13:00 UTC

These workflows use `gh workflow run` to trigger the main pipeline on the respective branch.

## Adding Support for New Major Versions

When DuckDB releases a new major version (e.g., 1.6):

1. Find the version branch codename at [DuckDB branches](https://github.com/duckdb/duckdb/branches)
   - Pattern: `v{major}.{minor}-{codename}`

2. Create a new branch from main:

   ```bash
   git checkout main
   git checkout -b v1.6
   ```

3. Update `MainDistributionPipeline.yml` on the new branch:

   ```yaml
   duckdb_version: v1.6-<codename>
   ci_tools_version: main
   ```

4. Create `scheduled-1.6.yml` on main branch (copy from existing scheduled workflow)

5. Push the branch:

   ```bash
   git push origin v1.6
   ```

## Reference

This strategy is based on the [tera extension](https://github.com/tera-insights/duckdb-tera) approach, which successfully maintains compatibility across DuckDB versions.
