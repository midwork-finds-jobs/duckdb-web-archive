SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Test: Verify both crawl_ids are in results
SELECT crawl_id, COUNT(*) as count
FROM common_crawl_index()
WHERE crawl_id IN ('CC-MAIN-2025-47', 'CC-MAIN-2025-38')
  AND url LIKE '%.teamtailor.com/%'
  AND status_code = 200
GROUP BY crawl_id
ORDER BY crawl_id;
