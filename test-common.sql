SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

-- Enable HTTP logging to see CDX requests with pushdown filters
SET enable_http_logging=true;

-- Query common_crawl_index for similar items as test-ia.sql
-- Note: Common Crawl uses full URLs (with https://) unlike Internet Archive (domain only)
-- Using teamtailor.com which has HTML pages in the crawl
SELECT url, timestamp, statuscode, mimetype
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-47'
  AND url LIKE '%.teamtailor.com/%'
  AND statuscode = 200
  AND mimetype = 'text/html'
LIMIT 15;
