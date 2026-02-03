# DuckDB Version Strategy

This extension uses a branch-based versioning strategy to automatically support new DuckDB patch releases.

## How It Works

DuckDB uses **version branches** with duck codenames:

- `v1.4-andium` → contains all 1.4.x releases (1.4.0, 1.4.1, 1.4.2, ...)
- `v1.5-variegata` → contains all 1.5.x releases
- `main` → development/nightly

By tracking these version branches instead of exact tags (e.g., `v1.4.2`), the extension automatically builds against new patch releases.

## Branch Strategy

| Branch | duckdb_version | Purpose |
|--------|----------------|---------|
| main | main | Track DuckDB development, early testing |
| v1.4 | v1.4-andium | Stable 1.4.x releases |
| v1.5 | v1.5-variegata | Stable 1.5.x releases |

## Scheduled Builds

Daily scheduled workflows trigger builds on stable branches:

- `scheduled-1.4.yml` - builds v1.4 branch daily at 12:00 UTC
- `scheduled-1.5.yml` - builds v1.5 branch daily at 13:00 UTC

This ensures the extension stays compatible as DuckDB releases patches.

## Adding Support for New Major Versions

When DuckDB releases a new major version (e.g., 1.6):

1. Create a new branch from main:

   ```bash
   git checkout main
   git checkout -b v1.6
   ```

2. Update `MainDistributionPipeline.yml` on the new branch:

   ```yaml
   duckdb_version: v1.6-<codename>
   ci_tools_version: main
   ```

3. Create `scheduled-1.6.yml` with appropriate cron schedule

4. Push the branch:

   ```bash
   git push origin v1.6
   ```

## Finding Version Branch Names

DuckDB version branch codenames can be found:

- In the [DuckDB repository branches](https://github.com/duckdb/duckdb/branches)
- Pattern: `v{major}.{minor}-{codename}`
