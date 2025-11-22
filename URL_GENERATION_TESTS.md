# CDX URL Generation Test Cases

This document describes the expected CDX API URLs that should be generated from different SQL queries.

## Test Case 1: Basic Query with Single Field

**SQL Query:**
```sql
SELECT url
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 5;
```

**Expected CDX URL:**
```
https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200
```

**Components:**
- Index: `CC-MAIN-2025-43-index`
- URL pattern: `*.example.com/*` (converted from `%.example.com/%`)
- Fields: `url` (only field requested)
- Limit: `10000` (default)
- Filter: `=statuscode:200`

---

## Test Case 2: Multiple Field Projection

**SQL Query:**
```sql
SELECT url, timestamp, status_code, mime_type
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 5;
```

**Expected CDX URL:**
```
https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url,timestamp,status_code,mime_type&limit=10000&filter==statuscode:200
```

**Components:**
- Fields: `url,timestamp,status_code,mime_type` (all requested fields)

---

## Test Case 3: Query with Response Column (WARC Fields)

**SQL Query:**
```sql
SELECT url, response
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 5;
```

**Expected CDX URL:**
```
https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url,filename,offset,length&limit=10000&filter==statuscode:200
```

**Components:**
- Fields: `url,filename,offset,length`
  - Note: `response` is not a CDX field, but requires `filename,offset,length` for WARC fetching

---

## Test Case 4: IN Clause with Multiple crawl_ids

**SQL Query:**
```sql
SELECT crawl_id, url
FROM common_crawl_index()
WHERE crawl_id IN ('CC-MAIN-2025-47', 'CC-MAIN-2025-43')
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 10;
```

**Expected CDX URLs (2 separate queries):**
```
https://index.commoncrawl.org/CC-MAIN-2025-47-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200
https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200
```

**Components:**
- Two separate queries, one for each crawl_id
- Results are combined in memory

---

## Test Case 5: Only crawl_id in SELECT

**SQL Query:**
```sql
SELECT crawl_id
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 5;
```

**Expected CDX URL:**
```
https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200
```

**Components:**
- Fields: `url` (fallback field, even though only crawl_id requested)
  - Note: `crawl_id` is not a CDX field, so we must request at least `url`

---

## Test Case 6: No status_code Filter

**SQL Query:**
```sql
SELECT url
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
LIMIT 5;
```

**Expected CDX URL:**
```
https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000
```

**Components:**
- No `filter=` parameter (status_code not specified)

---

## Test Case 7: Custom CDX Limit Parameter

**SQL Query:**
```sql
SELECT url
FROM common_crawl_index(500)  -- Custom limit
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 5;
```

**Expected CDX URL:**
```
https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=500&filter==statuscode:200
```

**Components:**
- Limit: `500` (custom value from function parameter)

---

## Test Case 8: No crawl_id (Latest from collinfo.json)

**SQL Query:**
```sql
SELECT url
FROM common_crawl_index()
WHERE url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 5;
```

**Expected Behavior:**
1. Fetch latest crawl_id from `https://index.commoncrawl.org/collinfo.json`
2. Cache result for 24 hours
3. Use latest crawl_id in URL

**Expected CDX URL (assuming latest is CC-MAIN-2025-47):**
```
https://index.commoncrawl.org/CC-MAIN-2025-47-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200
```

---

## Test Case 9: All Fields

**SQL Query:**
```sql
SELECT *
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 1;
```

**Expected CDX URL:**
```
https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url,filename,offset,length,timestamp,mime_type,digest,status_code&limit=10000&filter==statuscode:200
```

**Components:**
- Fields: All CDX fields except `crawl_id` (which is populated by the extension, not CDX)

---

## Test Case 10: Different status_code Values

**SQL Query:**
```sql
SELECT url
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 404
LIMIT 5;
```

**Expected CDX URL:**
```
https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:404
```

**Components:**
- Filter: `=statuscode:404` (custom status code)

---

## Running Tests

To manually verify URL generation, run queries with the extension and check stderr output for `[CDX URL]` lines:

```bash
./build/debug/duckdb -unsigned -f test_query.sql 2>&1 | grep "CDX URL"
```

## Implementation Notes

1. **URL Pattern Conversion**: SQL `LIKE` pattern `%.example.com/%` → CDX pattern `*.example.com/*`
2. **Field List (`fl=`)**: Only includes actual CDX fields, excludes:
   - `response` (computed from WARC file)
   - `crawl_id` (populated by extension from index name)
3. **Fallback Field**: If no CDX fields requested, defaults to `fl=url`
4. **WARC Fields**: When `response` is requested, automatically includes `filename,offset,length`
5. **Filter Pushdown**: `status_code` filters become `filter==statuscode:NNN`
6. **Multiple Crawls**: IN clause generates one query per crawl_id, results combined
