SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Query with automatic LIMIT pushdown (DuckDB v1.5+)
-- For v1.4.2: LIMIT is applied post-fetch, use WHERE crawl_id to limit scope
.timer on

SELECT url, timestamp, response.body
FROM common_crawl_index()  -- LIMIT will be pushed down to CDX API automatically
WHERE crawl_id IN ('CC-MAIN-2024-46', 'CC-MAIN-2024-42')  -- Valid 2024 crawl IDs
  AND url LIKE '%.teamtailor.com/%'
  AND status_code = 200
  AND mime_type != 'application/pdf'
LIMIT 10;