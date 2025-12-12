SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

-- Enable HTTP logging to see CDX requests with pushdown filters
--SET enable_http_logging=true;

CALL enable_logging('HTTP');

-- Query common_crawl_index for top-level pages only
-- Match URLs like https://www.teamtailor.com/ or https://2020change.teamtailor.com/
-- The LIKE provides the CDX url= parameter, SIMILAR TO filters to exact root paths
-- DuckDB SIMILAR TO uses regex syntax: .* = any chars, . = single char
SELECT url
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-47'
  AND url LIKE '%.teamtailor.com/%'
  AND url SIMILAR TO '.*teamtailor.com/$'
  AND statuscode = 200
  AND mimetype = 'text/html'

SELECT request.type, request.url
FROM duckdb_logs_parsed('HTTP');