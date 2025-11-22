# Performance Optimization Guide

## Understanding CDX Limit vs SQL LIMIT

The extension has two different limits:

### 1. CDX API Limit (controls how many records are fetched from index)
- Configured via function parameter: `common_crawl_index(N)`
- Default: 1000 records
- Affects: How many records are fetched from `index.commoncrawl.org`

### 2. SQL LIMIT (controls how many rows are returned to user)
- Configured in SQL: `LIMIT N`
- Affects: How many rows DuckDB returns from the query

## Performance Tips

### Small Result Sets (LIMIT < 100)

For queries with small LIMITs, use a small CDX limit to avoid fetching unnecessary records:

```sql
-- Bad: Fetches 1000 records from CDX, only uses 10
SELECT url FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
LIMIT 10;

-- Good: Fetches only 50 records from CDX
SELECT url FROM common_crawl_index(50)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
LIMIT 10;
```

**Rule of thumb**: Set CDX limit to 5-10x your SQL LIMIT for small queries.

### Large Result Sets (LIMIT > 1000)

For large result sets, increase the CDX limit:

```sql
-- Fetches up to 10000 records from CDX
SELECT url FROM common_crawl_index(10000)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
LIMIT 5000;
```

### Response Column (WARC Fetching)

When fetching `response` column, DuckDB automatically stops fetching WARC data once LIMIT is reached:

```sql
-- Only fetches WARC data for first 10 matching records
SELECT url, response FROM common_crawl_index(100)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
LIMIT 10;
```

**Performance**:
- CDX index queries (`index.commoncrawl.org`): Fast (~1-2 seconds)
- WARC fetches (`data.commoncrawl.org`): Slow (~100-500ms per record)
- Early termination: DuckDB stops calling scan() once LIMIT is satisfied

### Multiple Crawls (IN Clause)

With IN clause, each crawl_id is queried separately:

```sql
-- Fetches 100 records from EACH crawl_id (total: 200 from CDX)
SELECT url FROM common_crawl_index(100)
WHERE crawl_id IN ('CC-MAIN-2025-47', 'CC-MAIN-2025-43')
  AND url LIKE '%.example.com/%'
LIMIT 10;
```

**Tip**: Use smaller CDX limits with IN clauses since you're querying multiple indexes.

## Examples

### Example 1: Quick Sample (10 rows)
```sql
SELECT url, timestamp FROM common_crawl_index(50)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
LIMIT 10;
```
- CDX limit: 50 (5x SQL LIMIT)
- Fast: Fetches only 50 records from index

### Example 2: Moderate Sample (100 rows)
```sql
SELECT url, status_code FROM common_crawl_index(500)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
LIMIT 100;
```
- CDX limit: 500 (5x SQL LIMIT)

### Example 3: Large Dataset (all matching)
```sql
SELECT url, timestamp FROM common_crawl_index(100000)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%';
```
- CDX limit: 100000 (fetch many records)
- No SQL LIMIT: Return all matching

### Example 4: Expensive WARC Fetches
```sql
-- Only fetches 5 WARC responses (fast!)
SELECT url, response FROM common_crawl_index(20)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE 'https://example.com/page1'
LIMIT 5;
```
- CDX limit: 20 (conservative, exact URL match)
- DuckDB stops after 5 WARC fetches

## Why Can't We Auto-Detect SQL LIMIT?

DuckDB table functions don't have access to the SQL LIMIT clause during initialization. We must fetch records before knowing how many are needed. This is why the CDX limit parameter exists - to give you manual control.

## Monitoring Performance

Check debug output to see actual fetches:

```bash
./duckdb -unsigned -f your_query.sql 2>&1 | grep -E "CDX URL|records"
```

Look for:
- `[CDX URL]`: Shows the actual CDX query with limit parameter
- `Have N records to process`: Shows how many were fetched from CDX
- WARC fetches: Only happen for rows actually scanned

## Default Limit Rationale

Default of 1000 balances:
- Small queries: Acceptable overhead (1-2 seconds)
- Large queries: Users can increase with parameter
- Memory: ~1000 records ≈ few MB of memory
