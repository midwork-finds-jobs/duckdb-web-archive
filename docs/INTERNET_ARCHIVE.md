# Internet Archive CDX Table Function

## Overview

The `internet_archive()` table function provides access to the Internet Archive's Wayback Machine CDX (Capture Index) API, allowing you to query archived web pages and download their content.

**Key Features:**
- Query archived snapshots of any URL
- **Automatic LIMIT pushdown** - SQL LIMIT clauses sent directly to CDX API
- Filter by date range, status code, and MIME type
- Fetch archived page content directly
- Much simpler than Common Crawl - no WARC parsing needed
- Automatic filter pushdown to CDX API for efficiency
- Projection pushdown - response bodies only fetched when needed

## Basic Usage

```sql
-- Query archived snapshots with automatic LIMIT pushdown
SELECT url, timestamp, mimetype, statuscode
FROM internet_archive()
WHERE url LIKE 'archive.org'
  AND statuscode = 200
LIMIT 10;  -- LIMIT is automatically pushed to CDX API

-- Without LIMIT, fetches up to 10,000 CDX records (default)
SELECT url, timestamp, mimetype, statuscode
FROM internet_archive()
WHERE url LIKE 'archive.org'
  AND statuscode = 200;
```

## LIMIT Pushdown

The extension automatically pushes SQL `LIMIT` clauses to the CDX API for maximum efficiency:

```sql
-- This sends &limit=100 to the CDX API, not fetching 10,000 then limiting
SELECT * FROM internet_archive()
WHERE url = 'archive.org'
LIMIT 100;
```

**How it works**:
1. DuckDB's optimizer detects the LIMIT clause
2. The extension extracts the limit value during query planning
3. The limit is sent to the CDX API as `&limit=N`
4. The LIMIT node is removed from the query plan (no post-filtering needed)

## Optional Parameters

For advanced use cases, you can override the CDX API limit with a parameter:

```sql
internet_archive()      -- Default: 10,000 records, or SQL LIMIT if present
internet_archive(N)     -- Forces CDX API to fetch exactly N records
```

**When to use parameters**:
- When you want to fetch more records than your LIMIT (for caching/processing)
- When you need a specific CDX limit regardless of SQL LIMIT

```sql
-- Fetches 1000 from API, returns first 100
SELECT * FROM internet_archive(1000)
WHERE url = 'archive.org'
LIMIT 100;
```

## Schema

The function returns the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `url` | VARCHAR | Original URL that was archived |
| `timestamp` | TIMESTAMP_TZ | When the page was archived (UTC) |
| `urlkey` | VARCHAR | SURT-formatted URL key used for indexing |
| `mimetype` | VARCHAR | MIME type of the archived content |
| `statuscode` | INTEGER | HTTP status code (200, 404, etc.) |
| `digest` | VARCHAR | SHA-1 hash of the content |
| `length` | BIGINT | Content length in bytes |
| `response` | BLOB | Archived page content (raw HTTP body) |

## URL Matching

The function supports different URL matching modes via the `matchType` parameter, which is automatically detected from your WHERE clause pattern:

### Exact Match (default)
```sql
-- Matches exactly "archive.org"
SELECT * FROM internet_archive() WHERE url = 'archive.org' LIMIT 10;
```

### Prefix Match
```sql
-- Matches all URLs under archive.org/about/
SELECT * FROM internet_archive() WHERE url LIKE 'archive.org/about%' LIMIT 10;

-- Or using /* wildcard (equivalent)
SELECT * FROM internet_archive() WHERE url LIKE 'archive.org/about/*' LIMIT 10;
```

### Domain Match
```sql
-- Matches archive.org and all subdomains (*.archive.org)
SELECT * FROM internet_archive() WHERE url LIKE '*.archive.org' LIMIT 10;
```

### Host Match
```sql
-- Matches only the host archive.org (no subdomains)
SELECT * FROM internet_archive() WHERE url LIKE 'archive.org' LIMIT 10;
```

## Filtering

### Status Code Filtering
```sql
-- Only successful responses
SELECT * FROM internet_archive()
WHERE url = 'archive.org'
  AND statuscode = 200
LIMIT 10;

-- Exclude redirects
SELECT * FROM internet_archive()
WHERE url = 'archive.org'
  AND statuscode != 302
LIMIT 10;
```

### MIME Type Filtering
```sql
-- Only HTML pages
SELECT * FROM internet_archive()
WHERE url = 'archive.org'
  AND mimetype = 'text/html'
LIMIT 10;

-- Exclude PDFs
SELECT * FROM internet_archive()
WHERE url = 'archive.org'
  AND mimetype != 'application/pdf'
LIMIT 10;
```

### Combining Filters
```sql
-- HTML pages with 200 status from archive.org/about/
SELECT url, timestamp, length
FROM internet_archive()
WHERE url LIKE 'archive.org/about%'
  AND statuscode = 200
  AND mimetype = 'text/html'
LIMIT 20;
```

## Fetching Archived Content

The `response` column contains the raw HTTP body of the archived page:

```sql
-- Get the archived page content
SELECT url, timestamp, response
FROM internet_archive()
WHERE url = 'archive.org'
  AND statuscode = 200
LIMIT 1;

-- Save to file (using DuckDB COPY)
COPY (
  SELECT response
  FROM internet_archive()
  WHERE url = 'archive.org'
    AND statuscode = 200
  ORDER BY timestamp DESC
  LIMIT 1
) TO 'archived_page.html';

-- Get content size
SELECT url, timestamp, octet_length(response) as content_size
FROM internet_archive()
WHERE url = 'archive.org'
  AND statuscode = 200
LIMIT 5;
```

## Examples

### Find Latest Snapshot
```sql
-- Get the most recent archived version of a page
SELECT url, timestamp, mimetype, statuscode
FROM internet_archive()
WHERE url = 'archive.org/about/'
  AND statuscode = 200
ORDER BY timestamp DESC
LIMIT 1;
```

### Count Snapshots by Year
```sql
-- How many times was archive.org archived each year?
SELECT
  year(timestamp) as year,
  count(*) as snapshot_count
FROM internet_archive()
WHERE url = 'archive.org'
  AND statuscode = 200
GROUP BY year(timestamp)
ORDER BY year;
```

### Find All Archived Paths
```sql
-- Find all archived pages under archive.org/
SELECT DISTINCT url
FROM internet_archive()
WHERE url LIKE 'archive.org/%'
  AND statuscode = 200
LIMIT 100;
```

### Download Historical Snapshots
```sql
-- Get snapshots from different years
SELECT
  year(timestamp) as year,
  url,
  octet_length(response) as size_bytes
FROM internet_archive()
WHERE url = 'archive.org'
  AND statuscode = 200
  AND year(timestamp) IN (1997, 2000, 2010, 2020)
ORDER BY timestamp
LIMIT 10;
```

## Performance Tips

1. **Always specify a URL filter**: Don't query without a WHERE clause on `url`
   ```sql
   -- Bad: No URL filter (will query all of Archive.org!)
   SELECT * FROM internet_archive() LIMIT 10;

   -- Good: Specific URL filter
   SELECT * FROM internet_archive() WHERE url = 'archive.org' LIMIT 10;
   ```

2. **Use SQL LIMIT for efficiency**: LIMIT is automatically pushed down
   ```sql
   -- Only fetches 100 records from CDX API
   SELECT * FROM internet_archive()
   WHERE url = 'archive.org'
   LIMIT 100;

   -- Without LIMIT, fetches up to 10,000 records (default)
   SELECT * FROM internet_archive()
   WHERE url = 'archive.org';
   ```

3. **Push down filters**: Status code and MIME type filters are pushed down to the API
   ```sql
   -- Efficient: Filters pushed down to API
   SELECT * FROM internet_archive()
   WHERE url = 'archive.org'
     AND statuscode = 200
     AND mimetype = 'text/html'
   LIMIT 10;
   ```

4. **Projection pushdown (automatic)**: Response bodies only fetched when selected
   ```sql
   -- Fast: Response column not selected, no HTTP fetches
   SELECT url, timestamp, mimetype
   FROM internet_archive(1000)
   WHERE url = 'archive.org';

   -- Slow: Fetches 1000 response bodies from Archive.org
   SELECT url, response
   FROM internet_archive(1000)
   WHERE url = 'archive.org';
   ```

   The extension automatically detects if the `response` column is in your SELECT and only performs HTTP fetches when needed. This is **much faster** for metadata-only queries.

## Comparison with common_crawl_index()

| Feature | internet_archive() | common_crawl_index() |
|---------|-------------------|---------------------|
| Data Source | Wayback Machine | Common Crawl |
| Coverage | 1996-present | 2008-present (monthly) |
| URL Filtering | matchType parameter | Wildcard patterns |
| Data Retrieval | Direct HTTP (simple) | WARC files (complex) |
| Response Format | Raw HTTP body | Parsed WARC with headers |
| Speed | Faster for small queries | Optimized for bulk |
| Date Filtering | from/to parameters | crawl_id |

**Use internet_archive() when:**
- You need historical data from before 2008
- You want a specific version of a page
- You need simpler data retrieval
- You're querying a single domain

**Use common_crawl_index() when:**
- You need recent large-scale web data
- You want structured WARC/HTTP headers
- You need to process millions of pages
- You need specific crawl snapshots

## API Reference

### Base URL
`https://web.archive.org/cdx/search/cdx`

### Archived Page URL Format
`https://web.archive.org/web/{timestamp}id_/{original_url}`

**Note**: The `id_` suffix is critical - it returns the raw archived content without the Wayback Machine's UI wrapper.

### CDX API Parameters (automatically generated)

| Parameter | Description | Example |
|-----------|-------------|---------|
| `url` | URL to query | `url=archive.org` |
| `output` | Output format | `output=json` |
| `fl` | Fields to return | `fl=urlkey,timestamp,original,mimetype,statuscode,digest,length` |
| `matchType` | URL match mode | `matchType=prefix` |
| `filter` | Filter results | `filter=statuscode:200` |
| `limit` | Max results | `limit=10000` |

## Troubleshooting

### "400 Bad Request" Error
- Ensure you have a WHERE clause filtering by URL
- Check that your URL pattern is valid

### "No results returned"
- Verify the URL exists in the Wayback Machine: https://web.archive.org/
- Try removing filters (statuscode, mimetype) to see all snapshots
- Try different URL patterns (exact vs prefix vs domain)

### Slow Queries
- Don't fetch `response` column unless needed
- Use LIMIT to restrict result count
- Use specific filters (statuscode, mimetype)

## Further Reading

- [Wayback CDX Server API Documentation](https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server)
- [Internet Archive Web API](https://archive.org/developers/wayback-cdx-server.html)
- [CDX File Format](http://archive.org/web/researcher/cdx_file_format.php)
