SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Test: Query with multiple crawl_ids using IN clause
SELECT url, crawl_id, timestamp
FROM common_crawl_index()
WHERE crawl_id IN ('CC-MAIN-2025-47', 'CC-MAIN-2025-38')
  AND url LIKE '%.teamtailor.com/%'
  AND status_code = 200
LIMIT 5;
