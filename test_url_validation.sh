#!/usr/bin/env bash
# Quick test to verify CDX URL generation patterns
# Shows only the generated URLs without waiting for actual responses

set -e

DUCKDB="./build/debug/duckdb"

echo "==================================================================="
echo "CDX URL Generation Validation Tests"
echo "==================================================================="
echo ""

# Helper function to extract and show CDX URL
run_test() {
    local test_name="$1"
    local sql_query="$2"
    local expected_url="$3"

    echo "TEST: $test_name"
    echo "-------------------------------------------------------------------"

    # Create temp SQL file
    cat > /tmp/url_test.sql <<EOF
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

$sql_query
EOF

    echo "Expected URL:"
    echo "  $expected_url"
    echo ""
    echo "Actual URL(s):"

    # Run query and extract only CDX URLs
    timeout 10 $DUCKDB -unsigned -f /tmp/url_test.sql 2>&1 | grep "CDX URL" | sed 's/\[CDX URL\] /  /' || echo "  (timeout or no URL generated)"

    echo ""
    echo ""
}

# Test 1: Basic query with all common filters
run_test \
    "Basic query - url, crawl_id, status_code filters" \
    "SELECT url FROM common_crawl_index() WHERE crawl_id = 'CC-MAIN-2025-43' AND url LIKE '%.example.com/%' AND status_code = 200 LIMIT 1;" \
    "https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200"

# Test 2: Multiple fields
run_test \
    "Multiple field projection" \
    "SELECT url, timestamp, status_code FROM common_crawl_index() WHERE crawl_id = 'CC-MAIN-2025-43' AND url LIKE '%.example.com/%' AND status_code = 200 LIMIT 1;" \
    "https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url,timestamp,status_code&limit=10000&filter==statuscode:200"

# Test 3: Response field (should include warc fields)
run_test \
    "Query with response field" \
    "SELECT url, response FROM common_crawl_index() WHERE crawl_id = 'CC-MAIN-2025-43' AND url LIKE '%.example.com/%' AND status_code = 200 LIMIT 1;" \
    "https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url,filename,offset,length&limit=10000&filter==statuscode:200"

# Test 4: IN clause (multiple URLs)
run_test \
    "IN clause with 2 crawl_ids" \
    "SELECT crawl_id FROM common_crawl_index() WHERE crawl_id IN ('CC-MAIN-2025-47', 'CC-MAIN-2025-43') AND url LIKE '%.example.com/%' AND status_code = 200 LIMIT 1;" \
    "Should show 2 URLs (one for each crawl_id)"

# Test 5: Only crawl_id projection
run_test \
    "Only crawl_id in SELECT" \
    "SELECT crawl_id FROM common_crawl_index() WHERE crawl_id = 'CC-MAIN-2025-43' AND url LIKE '%.example.com/%' AND status_code = 200 LIMIT 1;" \
    "https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000&filter==statuscode:200"

# Test 6: No status_code filter
run_test \
    "Without status_code filter" \
    "SELECT url FROM common_crawl_index() WHERE crawl_id = 'CC-MAIN-2025-43' AND url LIKE '%.example.com/%' LIMIT 1;" \
    "https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=10000"

# Test 7: Custom limit
run_test \
    "Custom CDX limit (500)" \
    "SELECT url FROM common_crawl_index(500) WHERE crawl_id = 'CC-MAIN-2025-43' AND url LIKE '%.example.com/%' AND status_code = 200 LIMIT 1;" \
    "https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url&limit=500&filter==statuscode:200"

# Cleanup
rm -f /tmp/url_test.sql

echo "==================================================================="
echo "URL Validation Tests Complete"
echo "==================================================================="
