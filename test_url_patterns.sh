#!/usr/bin/env bash
# Test script to verify CDX URL generation from different SQL queries

set -e

DUCKDB="./build/debug/duckdb"
EXT_PATH="build/debug/extension/common_crawl/common_crawl.duckdb_extension"

echo "==================================================================="
echo "Testing CDX URL Generation from Different SQL Queries"
echo "==================================================================="
echo ""

# Test 1: Basic query with single field projection
echo "TEST 1: Single field projection (url only)"
echo "-------------------------------------------------------------------"
cat > /tmp/test1.sql <<'EOF'
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

SELECT url
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 1;
EOF

echo "Expected URL: https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200"
echo "Actual URL:"
$DUCKDB -unsigned -f /tmp/test1.sql 2>&1 | grep "CDX URL" || echo "No CDX URL found"
echo ""

# Test 2: Multiple field projection
echo "TEST 2: Multiple field projection (url, timestamp, status_code)"
echo "-------------------------------------------------------------------"
cat > /tmp/test2.sql <<'EOF'
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

SELECT url, timestamp, status_code
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 1;
EOF

echo "Expected URL: https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url,timestamp,status_code&limit=10000&filter==statuscode:200"
echo "Actual URL:"
$DUCKDB -unsigned -f /tmp/test2.sql 2>&1 | grep "CDX URL" || echo "No CDX URL found"
echo ""

# Test 3: Query with response column (should include warc fields)
echo "TEST 3: Query with response column (should include filename, offset, length)"
echo "-------------------------------------------------------------------"
cat > /tmp/test3.sql <<'EOF'
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

SELECT url, response
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 1;
EOF

echo "Expected URL: https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url,filename,offset,length&limit=10000&filter==statuscode:200"
echo "Actual URL:"
$DUCKDB -unsigned -f /tmp/test3.sql 2>&1 | grep "CDX URL" || echo "No CDX URL found"
echo ""

# Test 4: IN clause with multiple crawl_ids
echo "TEST 4: IN clause with multiple crawl_ids (should generate 2 URLs)"
echo "-------------------------------------------------------------------"
cat > /tmp/test4.sql <<'EOF'
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

SELECT crawl_id, COUNT(*) as count
FROM common_crawl_index()
WHERE crawl_id IN ('CC-MAIN-2025-47', 'CC-MAIN-2025-43')
  AND url LIKE '%.example.com/%'
  AND status_code = 200
GROUP BY crawl_id
LIMIT 10;
EOF

echo "Expected URLs:"
echo "  1. https://index.commoncrawl.org/CC-MAIN-2025-47-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200"
echo "  2. https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200"
echo "Actual URLs:"
$DUCKDB -unsigned -f /tmp/test4.sql 2>&1 | grep "CDX URL" || echo "No CDX URL found"
echo ""

# Test 5: Query with only crawl_id projection (should still request url)
echo "TEST 5: Query with only crawl_id projection (should request url field)"
echo "-------------------------------------------------------------------"
cat > /tmp/test5.sql <<'EOF'
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

SELECT crawl_id
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 1;
EOF

echo "Expected URL: https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200"
echo "Actual URL:"
$DUCKDB -unsigned -f /tmp/test5.sql 2>&1 | grep "CDX URL" || echo "No CDX URL found"
echo ""

# Test 6: Query without status_code filter (should not have filter= parameter)
echo "TEST 6: Query without status_code filter"
echo "-------------------------------------------------------------------"
cat > /tmp/test6.sql <<'EOF'
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

SELECT url
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
LIMIT 1;
EOF

echo "Expected URL: https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000"
echo "Actual URL:"
$DUCKDB -unsigned -f /tmp/test6.sql 2>&1 | grep "CDX URL" || echo "No CDX URL found"
echo ""

# Test 7: Query with custom limit parameter
echo "TEST 7: Query with custom CDX limit (100)"
echo "-------------------------------------------------------------------"
cat > /tmp/test7.sql <<'EOF'
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

SELECT url
FROM common_crawl_index(100)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 1;
EOF

echo "Expected URL: https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=100&filter==statuscode:200"
echo "Actual URL:"
$DUCKDB -unsigned -f /tmp/test7.sql 2>&1 | grep "CDX URL" || echo "No CDX URL found"
echo ""

# Test 8: Query without crawl_id (should fetch latest)
echo "TEST 8: Query without explicit crawl_id (should fetch latest from collinfo.json)"
echo "-------------------------------------------------------------------"
cat > /tmp/test8.sql <<'EOF'
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

SELECT url
FROM common_crawl_index()
WHERE url LIKE '%.example.com/%'
  AND status_code = 200
LIMIT 1;
EOF

echo "Expected: Should see 'Fetching latest crawl_id from collinfo.json' followed by CDX URL with latest crawl"
echo "Actual:"
$DUCKDB -unsigned -f /tmp/test8.sql 2>&1 | grep -E "(Fetching latest|CDX URL)" || echo "No output found"
echo ""

# Cleanup
rm -f /tmp/test*.sql

echo "==================================================================="
echo "URL Generation Tests Complete"
echo "==================================================================="
