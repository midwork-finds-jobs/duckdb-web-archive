SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Test 1: Basic query with URL filter and crawl_id
-- Expected URL: https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200
SELECT url
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 5;
