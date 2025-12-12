#include "common_crawl_utils.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

// ========================================
// BIND DATA AND STATE
// ========================================

// Structure to hold bind data for the table function
struct CommonCrawlBindData : public TableFunctionData {
	string index_name;  // Single index (for backwards compatibility)
	vector<string> crawl_ids;  // Multiple crawl_ids for IN clause support
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> fields_needed;
	bool fetch_response;
	string url_filter;
	vector<string> cdx_filters; // CDX API filter parameters (e.g., "=status:200", "=mime:text/html")
	idx_t max_results; // Maximum number of results to fetch from CDX API

	// Default CDX limit set to 100 to prevent fetching too many results
	CommonCrawlBindData(string index) : index_name(std::move(index)), fetch_response(false), url_filter("*"), max_results(100) {}
};

// Structure to hold global state for the table function
struct CommonCrawlGlobalState : public GlobalTableFunctionState {
	vector<CDXRecord> records;
	idx_t current_position;
	vector<column_t> column_ids; // Which columns are actually selected

	CommonCrawlGlobalState() : current_position(0) {}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for now
	}
};

// ========================================
// CDX API QUERY
// ========================================

// Helper function to query CDX API using FileSystem
static vector<CDXRecord> QueryCDXAPI(ClientContext &context, const string &index_name, const string &url_pattern,
                                      const vector<string> &fields_needed, const vector<string> &cdx_filters, idx_t max_results) {
	fprintf(stderr, "[DEBUG] QueryCDXAPI started\n");
	vector<CDXRecord> records;

	// Helper lambda to map DuckDB column names to CDX API field names
	auto map_column_to_field = [](const string &col_name) -> string {
		if (col_name == "mimetype") return "mime";
		if (col_name == "statuscode") return "status";
		return col_name; // url, digest, timestamp, filename, offset, length stay the same
	};

	// Construct field list for &fl= parameter to optimize the query
	// Only request fields that are actually needed
	string field_list = "";
	bool need_warc_fields = false;
	for (size_t i = 0; i < fields_needed.size(); i++) {
		if (i > 0) {
			field_list += ",";
		}
		field_list += map_column_to_field(fields_needed[i]);

		// Check if we need WARC fields for parsing
		if (fields_needed[i] == "filename" || fields_needed[i] == "offset" || fields_needed[i] == "length") {
			need_warc_fields = true;
		}
	}

	// Construct the CDX API URL
	// Add limit parameter to control how many results we fetch
	string cdx_url = "https://index.commoncrawl.org/" + index_name + "-index?url=" + url_pattern +
	                 "&output=json&fl=" + field_list + "&limit=" + to_string(max_results);

	// Add filter parameters (e.g., filter==statuscode:200)
	for (const auto &filter : cdx_filters) {
		cdx_url += "&filter=" + filter;
	}

	// Debug: print the final CDX URL
	fprintf(stderr, "[CDX URL +%.0fms] %s\n", ElapsedMs(), cdx_url.c_str());

	try {
		fprintf(stderr, "[DEBUG] Opening CDX URL\n");
		// Use FileSystem to fetch the CDX data
		auto &fs = FileSystem::GetFileSystem(context);
		auto file_handle = fs.OpenFile(cdx_url, FileFlags::FILE_FLAGS_READ);

		fprintf(stderr, "[DEBUG] Reading CDX response\n");
		// Read the entire response
		string response_data;
		const idx_t buffer_size = 8192;
		auto buffer = unique_ptr<char[]>(new char[buffer_size]);

		while (true) {
			int64_t bytes_read = file_handle->Read(buffer.get(), buffer_size);
			if (bytes_read <= 0) {
				break;
			}
			response_data.append(buffer.get(), bytes_read);
		}

		fprintf(stderr, "[DEBUG] Got %lu bytes, sanitizing UTF-8\n", (unsigned long)response_data.size());
		// Sanitize the entire response to ensure valid UTF-8
		response_data = SanitizeUTF8(response_data);
		fprintf(stderr, "[DEBUG] UTF-8 sanitization complete\n");

		// Parse newline-delimited JSON
		fprintf(stderr, "[DEBUG] Parsing JSON lines\n");
		std::istringstream stream(response_data);
		string line;
		int line_count = 0;

		while (std::getline(stream, line)) {
			if (line.empty() || line[0] != '{') {
				continue;
			}
			line_count++;

			CDXRecord record;

			// Extract fields from JSON line
			record.url = ExtractJSONValue(line, "url");
			if (record.url.empty()) {
				continue; // Skip invalid records
			}

			record.timestamp = ExtractJSONValue(line, "timestamp");
			record.mime_type = ExtractJSONValue(line, "mime");
			record.digest = ExtractJSONValue(line, "digest");
			record.crawl_id = index_name;

			string status_str = ExtractJSONValue(line, "status");
			record.status_code = status_str.empty() ? 0 : std::stoi(status_str);

			if (need_warc_fields) {
				record.filename = ExtractJSONValue(line, "filename");
				string offset_str = ExtractJSONValue(line, "offset");
				string length_str = ExtractJSONValue(line, "length");

				record.offset = offset_str.empty() ? 0 : std::stoll(offset_str);
				record.length = length_str.empty() ? 0 : std::stoll(length_str);
			}

			records.push_back(record);
		}
		fprintf(stderr, "[DEBUG] Parsed %d JSON lines, got %lu records\n", line_count, (unsigned long)records.size());

	} catch (std::exception &ex) {
		throw IOException("Error querying CDX API: " + string(ex.what()));
	} catch (...) {
		throw IOException("Unknown error querying CDX API");
	}

	return records;
}

// ========================================
// WARC FETCHING
// ========================================

// Helper function to fetch WARC response using FileSystem API
static WARCResponse FetchWARCResponse(ClientContext &context, const CDXRecord &record) {
	WARCResponse result;

	if (record.filename.empty() || record.offset == 0 || record.length == 0) {
		return result; // Invalid record - return empty
	}

	try {
		// Construct the WARC URL
		string warc_url = "https://data.commoncrawl.org/" + record.filename;

		// Get the file system from the database context
		auto &fs = FileSystem::GetFileSystem(context);

		// Open the file through httpfs (HTTP URLs are handled when httpfs is loaded)
		auto file_handle = fs.OpenFile(warc_url, FileFlags::FILE_FLAGS_READ);

		// Allocate buffer for the compressed data
		auto buffer = unique_ptr<char[]>(new char[record.length]);

		// Seek to the offset and read the specified length
		// httpfs should translate this into HTTP Range request: bytes=offset-(offset+length-1)
		file_handle->Seek(record.offset);
		int64_t bytes_read = file_handle->Read(buffer.get(), record.length);

		if (bytes_read <= 0) {
			result.body = "[Error: Failed to read data from WARC file]";
			return result;
		}

		// The data we read is gzip compressed
		// We need to decompress it to get the WARC content
		string decompressed = DecompressGzip(buffer.get(), bytes_read);

		// Parse the WARC format to extract HTTP response headers and body
		if (decompressed.find("[Error") == 0) {
			// If decompression returned an error message, put it in body
			result.body = decompressed;
			return result;
		}

		return ParseWARCResponse(decompressed);

	} catch (Exception &ex) {
		// Return error message in body for debugging
		result.body = "[Error fetching WARC: " + string(ex.what()) + "]";
		return result;
	} catch (std::exception &ex) {
		result.body = "[Error fetching WARC: " + string(ex.what()) + "]";
		return result;
	} catch (...) {
		result.body = "[Unknown error fetching WARC]";
		return result;
	}
}

// ========================================
// TABLE FUNCTION IMPLEMENTATION
// ========================================

// Bind function for the table function
static unique_ptr<FunctionData> CommonCrawlBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	fprintf(stderr, "[DEBUG] CommonCrawlBind called\n");
	fflush(stderr);

	// Optional max_results named parameter to control CDX API result size
	// If provided, overrides the default max_results (100)
	auto bind_data = make_uniq<CommonCrawlBindData>("");

	// Handle named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "max_results") {
			if (kv.second.type().id() != LogicalTypeId::BIGINT) {
				throw BinderException("common_crawl_index max_results parameter must be an integer");
			}
			bind_data->max_results = kv.second.GetValue<int64_t>();
			fprintf(stderr, "[DEBUG] CDX API max_results set to: %lu\n", (unsigned long)bind_data->max_results);
		} else {
			throw BinderException("Unknown parameter '%s' for common_crawl_index", kv.first.c_str());
		}
	}

	// Define output columns
	names.push_back("url");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("url");

	names.push_back("timestamp");
	return_types.push_back(LogicalType::TIMESTAMP_TZ);
	bind_data->fields_needed.push_back("timestamp");

	names.push_back("mimetype");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("mimetype");

	names.push_back("statuscode");
	return_types.push_back(LogicalType::INTEGER);
	bind_data->fields_needed.push_back("statuscode");

	names.push_back("digest");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("digest");

	names.push_back("filename");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("filename");

	names.push_back("offset");
	return_types.push_back(LogicalType::BIGINT);
	bind_data->fields_needed.push_back("offset");

	names.push_back("length");
	return_types.push_back(LogicalType::BIGINT);
	bind_data->fields_needed.push_back("length");

	// Add crawl_id column (populated from index_name)
	names.push_back("crawl_id");
	return_types.push_back(LogicalType::VARCHAR);

	// Add warc STRUCT with WARC metadata
	// Fields: version (VARCHAR), headers (MAP)
	names.push_back("warc");
	child_list_t<LogicalType> warc_children;
	warc_children.push_back(make_pair("version", LogicalType::VARCHAR));
	warc_children.push_back(make_pair("headers", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
	return_types.push_back(LogicalType::STRUCT(warc_children));

	// Add response STRUCT with HTTP response details
	// Fields: body (BLOB), headers (MAP), http_version (VARCHAR)
	names.push_back("response");
	child_list_t<LogicalType> response_children;
	response_children.push_back(make_pair("body", LogicalType::BLOB));
	response_children.push_back(make_pair("headers", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
	response_children.push_back(make_pair("http_version", LogicalType::VARCHAR));
	return_types.push_back(LogicalType::STRUCT(response_children));

	// Enable response fetching (implemented with HTTP range requests + gzip decompression)
	// Note: This will fetch WARC files which can be slow for large result sets
	// Projection pushdown will control whether this is actually fetched
	bind_data->fetch_response = true;

	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

// Init global state function
static unique_ptr<GlobalTableFunctionState> CommonCrawlInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	fprintf(stderr, "[DEBUG] CommonCrawlInitGlobal called\n");
	auto &bind_data = const_cast<CommonCrawlBindData&>(input.bind_data->Cast<CommonCrawlBindData>());
	auto state = make_uniq<CommonCrawlGlobalState>();

	// If no crawl_id was specified in WHERE clause, fetch the latest one
	// Don't fetch if crawl_ids vector is populated (from IN clause)
	if (bind_data.index_name.empty() && bind_data.crawl_ids.empty()) {
		bind_data.index_name = GetLatestCrawlId(context);
		fprintf(stderr, "[DEBUG] Using latest crawl_id: %s\n", bind_data.index_name.c_str());
	}

	// Store which columns are selected for projection pushdown
	state->column_ids = input.column_ids;
	fprintf(stderr, "[DEBUG] Projected columns: ");
	for (auto &col_id : input.column_ids) {
		fprintf(stderr, "%lu ", (unsigned long)col_id);
	}
	fprintf(stderr, "\n");

	// CDX API fields that can be requested from index.commoncrawl.org
	// All other columns are computed by our extension
	static const std::unordered_set<string> cdx_fields = {
		"url", "timestamp", "mimetype", "statuscode",
		"digest", "filename", "offset", "length"
	};

	// Determine which fields are actually needed based on projection
	vector<string> needed_fields;
	bool need_response = false;
	bool need_warc = false;

	for (auto &col_id : input.column_ids) {
		if (col_id < bind_data.column_names.size()) {
			string col_name = bind_data.column_names[col_id];
			fprintf(stderr, "[DEBUG] Column %lu = %s\n", (unsigned long)col_id, col_name.c_str());

			// Check if we need to fetch WARC data for this column
			if (col_name == "warc" || col_name == "response") {
				need_response = true;
				need_warc = true; // WARC response parsing requires filename/offset/length
			} else if (col_name == "filename" || col_name == "offset" || col_name == "length") {
				need_warc = true;
			}

			// Only add CDX fields to needed_fields (whitelist approach)
			// Non-CDX columns are computed by the extension:
			// - crawl_id (from index name)
			// - warc, response (from WARC file)
			if (cdx_fields.count(col_name) > 0) {
				needed_fields.push_back(col_name);
			}
		}
	}

	// If we need WARC data, ensure we have filename, offset, length
	if (need_warc) {
		if (std::find(needed_fields.begin(), needed_fields.end(), "filename") == needed_fields.end()) {
			needed_fields.push_back("filename");
		}
		if (std::find(needed_fields.begin(), needed_fields.end(), "offset") == needed_fields.end()) {
			needed_fields.push_back("offset");
		}
		if (std::find(needed_fields.begin(), needed_fields.end(), "length") == needed_fields.end()) {
			needed_fields.push_back("length");
		}
	}

	// Override fetch_response based on projection
	bind_data.fetch_response = need_response;
	fprintf(stderr, "[DEBUG] fetch_response = %d, need_warc = %d\n", bind_data.fetch_response, need_warc);

	// Ensure we always request at least 'url' field from CDX API
	// (e.g., when only crawl_id is selected, we still need a CDX field to get records)
	if (needed_fields.empty()) {
		needed_fields.push_back("url");
	}

	// Use the URL filter from bind data (could be set via filter pushdown)
	string url_pattern = bind_data.url_filter;
	fprintf(stderr, "[DEBUG] About to call QueryCDXAPI with %lu fields\n", (unsigned long)needed_fields.size());

	// Query CDX API - handle multiple crawl_ids if IN clause was used
	if (!bind_data.crawl_ids.empty()) {
		// IN clause detected: query each crawl_id in parallel and combine results
		fprintf(stderr, "[DEBUG] Launching %lu parallel CDX API requests\n", (unsigned long)bind_data.crawl_ids.size());

		std::vector<std::future<vector<CDXRecord>>> futures;
		futures.reserve(bind_data.crawl_ids.size());

		// Launch async requests for each crawl_id
		for (const auto &crawl_id : bind_data.crawl_ids) {
			futures.push_back(std::async(std::launch::async, [&context, crawl_id, url_pattern, &needed_fields, &bind_data]() {
				return QueryCDXAPI(context, crawl_id, url_pattern, needed_fields, bind_data.cdx_filters, bind_data.max_results);
			}));
		}

		// Collect results from all futures
		for (auto &future : futures) {
			auto records = future.get();
			state->records.insert(state->records.end(), records.begin(), records.end());
		}

		fprintf(stderr, "[DEBUG] All parallel requests completed, total records: %lu\n", (unsigned long)state->records.size());
	} else {
		// Single crawl_id: use index_name
		state->records = QueryCDXAPI(context, bind_data.index_name, url_pattern, needed_fields, bind_data.cdx_filters, bind_data.max_results);
		fprintf(stderr, "[DEBUG] QueryCDXAPI returned %lu records\n", (unsigned long)state->records.size());
	}

	return std::move(state);
}

// Scan function for the table function
static void CommonCrawlScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	fprintf(stderr, "[DEBUG] CommonCrawlScan called\n");
	auto &bind_data = data.bind_data->Cast<CommonCrawlBindData>();
	auto &gstate = data.global_state->Cast<CommonCrawlGlobalState>();
	fprintf(stderr, "[DEBUG] Have %lu records to process\n", (unsigned long)gstate.records.size());

	// Pre-fetch WARCs in parallel for this chunk if needed
	std::vector<WARCResponse> warc_responses;
	idx_t chunk_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, gstate.records.size() - gstate.current_position);

	if (bind_data.fetch_response && chunk_size > 0) {
		fprintf(stderr, "[DEBUG] Pre-fetching %lu WARCs in parallel\n", (unsigned long)chunk_size);
		std::vector<std::future<WARCResponse>> warc_futures;
		warc_futures.reserve(chunk_size);

		// Launch parallel WARC fetches
		for (idx_t i = 0; i < chunk_size; i++) {
			auto &record = gstate.records[gstate.current_position + i];
			warc_futures.push_back(std::async(std::launch::async, [&context, record]() {
				return FetchWARCResponse(context, record);
			}));
		}

		// Collect results
		warc_responses.reserve(chunk_size);
		for (auto &future : warc_futures) {
			warc_responses.push_back(future.get());
		}
		fprintf(stderr, "[DEBUG] All %lu WARCs fetched\n", (unsigned long)chunk_size);
	}

	idx_t output_offset = 0;
	while (gstate.current_position < gstate.records.size() && output_offset < STANDARD_VECTOR_SIZE) {
		auto &record = gstate.records[gstate.current_position];

		bool row_success = true;

		// Process each projected column
		for (idx_t proj_idx = 0; proj_idx < gstate.column_ids.size(); proj_idx++) {
			auto col_id = gstate.column_ids[proj_idx];
			string col_name = bind_data.column_names[col_id];

			try {
				if (col_name == "url") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.url));
				} else if (col_name == "timestamp") {
					auto data_ptr = FlatVector::GetData<timestamp_t>(output.data[proj_idx]);
					data_ptr[output_offset] = ParseCDXTimestamp(record.timestamp);
				} else if (col_name == "mimetype") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.mime_type));
				} else if (col_name == "statuscode") {
					auto data_ptr = FlatVector::GetData<int32_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.status_code;
				} else if (col_name == "digest") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.digest));
				} else if (col_name == "filename") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.filename));
				} else if (col_name == "offset") {
					auto data_ptr = FlatVector::GetData<int64_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.offset;
				} else if (col_name == "length") {
					auto data_ptr = FlatVector::GetData<int64_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.length;
				} else if (col_name == "crawl_id") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], record.crawl_id);
				} else if (col_name == "warc" || col_name == "response") {
					if (bind_data.fetch_response && !warc_responses.empty()) {
						WARCResponse &warc_response = warc_responses[output_offset];

						if (col_name == "warc") {
							// WARC STRUCT with version and headers
							auto &struct_vector = output.data[proj_idx];
							auto &struct_children = StructVector::GetEntries(struct_vector);

							// Child 0: version (VARCHAR)
							auto &version_vector = struct_children[0];
							auto version_data = FlatVector::GetData<string_t>(*version_vector);
							version_data[output_offset] = StringVector::AddString(*version_vector,
							                                                        SanitizeUTF8(warc_response.warc_version));

							// Child 1: headers (MAP)
							auto &headers_map = struct_children[1];
							auto &map_keys = MapVector::GetKeys(*headers_map);
							auto &map_values = MapVector::GetValues(*headers_map);

							idx_t map_offset = ListVector::GetListSize(*headers_map);
							idx_t new_size = map_offset + warc_response.warc_headers.size();
							ListVector::Reserve(*headers_map, new_size);

							auto key_data = FlatVector::GetData<string_t>(map_keys);
							auto value_data = FlatVector::GetData<string_t>(map_values);

							for (const auto &header : warc_response.warc_headers) {
								key_data[map_offset] = StringVector::AddString(map_keys, header.first);
								value_data[map_offset] = StringVector::AddString(map_values, SanitizeUTF8(header.second));
								map_offset++;
							}

							auto map_data = FlatVector::GetData<list_entry_t>(*headers_map);
							map_data[output_offset].offset = ListVector::GetListSize(*headers_map);
							map_data[output_offset].length = warc_response.warc_headers.size();
							ListVector::SetListSize(*headers_map, map_offset);

						} else if (col_name == "response") {
							// Response STRUCT with body, headers, http_version
							auto &struct_vector = output.data[proj_idx];
							auto &struct_children = StructVector::GetEntries(struct_vector);

							// Child 0: body (BLOB)
							auto &body_vector = struct_children[0];
							auto body_data = FlatVector::GetData<string_t>(*body_vector);
							body_data[output_offset] = StringVector::AddStringOrBlob(*body_vector, warc_response.body);

							// Child 1: headers (MAP)
							auto &headers_map = struct_children[1];
							auto &map_keys = MapVector::GetKeys(*headers_map);
							auto &map_values = MapVector::GetValues(*headers_map);

							idx_t map_offset = ListVector::GetListSize(*headers_map);
							idx_t new_size = map_offset + warc_response.http_headers.size();
							ListVector::Reserve(*headers_map, new_size);

							auto key_data = FlatVector::GetData<string_t>(map_keys);
							auto value_data = FlatVector::GetData<string_t>(map_values);

							for (const auto &header : warc_response.http_headers) {
								key_data[map_offset] = StringVector::AddString(map_keys, header.first);
								value_data[map_offset] = StringVector::AddString(map_values, SanitizeUTF8(header.second));
								map_offset++;
							}

							auto map_data = FlatVector::GetData<list_entry_t>(*headers_map);
							map_data[output_offset].offset = ListVector::GetListSize(*headers_map);
							map_data[output_offset].length = warc_response.http_headers.size();
							ListVector::SetListSize(*headers_map, map_offset);

							// Child 2: http_version (VARCHAR)
							auto &version_vector = struct_children[2];
							auto version_data = FlatVector::GetData<string_t>(*version_vector);
							version_data[output_offset] = StringVector::AddString(*version_vector,
							                                                        SanitizeUTF8(warc_response.http_version));
						}
					} else {
						FlatVector::SetNull(output.data[proj_idx], output_offset, true);
					}
				}
			} catch (const std::exception &ex) {
				fprintf(stderr, "[ERROR] Failed to process column %s for row %lu: %s\n",
				        col_name.c_str(), (unsigned long)gstate.current_position, ex.what());
				row_success = false;
				break;
			}
		}

		if (row_success) {
			output_offset++;
		}
		gstate.current_position++;
	}

	output.SetCardinality(output_offset);
}

// ========================================
// FILTER PUSHDOWN
// ========================================

// Filter pushdown function to handle WHERE clauses
static void CommonCrawlPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                               vector<unique_ptr<Expression>> &filters) {
	fprintf(stderr, "[DEBUG] CommonCrawlPushdownComplexFilter called with %lu filters\n", (unsigned long)filters.size());
	auto &bind_data = bind_data_p->Cast<CommonCrawlBindData>();


	// Build a map of column names to their indices
	std::unordered_map<string, idx_t> column_map;
	for (idx_t i = 0; i < bind_data.column_names.size(); i++) {
		column_map[bind_data.column_names[i]] = i;
	}

	// Look for filters we can push down
	vector<idx_t> filters_to_remove;

	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		fprintf(stderr, "[DEBUG] Filter %lu: class=%d\n",
		        (unsigned long)i, (int)filter->GetExpressionClass());

		// Handle BOUND_OPERATOR for IN clauses (e.g., crawl_id IN ('id1', 'id2'))
		// DuckDB represents IN as a BOUND_OPERATOR with children: [column, value1, value2, ...]
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
			auto &op = filter->Cast<BoundOperatorExpression>();

			// Check if first child is a crawl_id column and rest are constants
			if (op.children.size() >= 2 &&
			    op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				auto &col_ref = op.children[0]->Cast<BoundColumnRefExpression>();

				if (col_ref.GetName() == "crawl_id") {
					vector<string> crawl_id_values;
					bool all_constants = true;

					// Extract all constant values (children[1] onwards)
					for (size_t j = 1; j < op.children.size(); j++) {
						if (op.children[j]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
							auto &constant = op.children[j]->Cast<BoundConstantExpression>();
							if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
								crawl_id_values.push_back(constant.value.ToString());
								continue;
							}
						}
						all_constants = false;
						break;
					}

					if (all_constants && !crawl_id_values.empty()) {
						bind_data.crawl_ids = crawl_id_values;
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}

			// Alternative: Check if this is an IN operator (children[0] should be wrapped in CONJUNCTION due to optimization)
			if (op.children.size() == 1 &&
			    op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
				auto &conjunction = op.children[0]->Cast<BoundConjunctionExpression>();
				fprintf(stderr, "[DEBUG] Found CONJUNCTION inside OPERATOR, type=%d, children=%lu\n",
				        (int)conjunction.type, (unsigned long)conjunction.children.size());

				// Only handle OR conjunctions (IN clauses become OR of equalities)
				if (conjunction.type == ExpressionType::CONJUNCTION_OR) {
					vector<string> crawl_id_values;
					bool all_crawl_id_comparisons = true;

					// Check if all children are crawl_id = 'value' comparisons
					for (auto &child : conjunction.children) {
						if (child->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON &&
						    child->type == ExpressionType::COMPARE_EQUAL) {
							auto &comp = child->Cast<BoundComparisonExpression>();

							if (comp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
							    comp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
								auto &col_ref = comp.left->Cast<BoundColumnRefExpression>();
								auto &constant = comp.right->Cast<BoundConstantExpression>();

								if (col_ref.GetName() == "crawl_id" &&
								    constant.value.type().id() == LogicalTypeId::VARCHAR) {
									crawl_id_values.push_back(constant.value.ToString());
									continue;
								}
							}
						}
						all_crawl_id_comparisons = false;
						break;
					}

					// If we successfully extracted all crawl_id values from IN clause
					if (all_crawl_id_comparisons && !crawl_id_values.empty()) {
						bind_data.crawl_ids = crawl_id_values;
						fprintf(stderr, "[DEBUG] IN clause detected with %lu crawl_ids\n",
						        (unsigned long)crawl_id_values.size());
						for (const auto &id : crawl_id_values) {
							fprintf(stderr, "[DEBUG]   - %s\n", id.c_str());
						}
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
		}

		// Handle BOUND_FUNCTION for LIKE expressions
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func = filter->Cast<BoundFunctionExpression>();
			fprintf(stderr, "[DEBUG] Function name: %s\n", func.function.name.c_str());

			// Check if this is a CONTAINS function (DuckDB optimizes LIKE '%string%' to contains)
			if (func.function.name == "contains") {
				// CONTAINS has 2 children: column and search string
				if (func.children.size() >= 2 &&
				    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string search_string = constant.value.ToString();
						fprintf(stderr, "[DEBUG] CONTAINS URL filter search string: '%s'\n", search_string.c_str());
						// Convert to CDX wildcard pattern: *search_string*
						bind_data.url_filter = "*" + search_string + "*";
						fprintf(stderr, "[DEBUG] CONTAINS URL filter pattern: '%s'\n", bind_data.url_filter.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
			// Check if this is a LIKE function
			else if (func.function.name == "like" || func.function.name == "~~") {
				// LIKE has 2 children: column and pattern
				if (func.children.size() >= 2 &&
				    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string original_pattern = constant.value.ToString();
						fprintf(stderr, "[DEBUG] LIKE URL filter original pattern: '%s'\n", original_pattern.c_str());
						// Convert SQL LIKE wildcards (%) to CDX API wildcards (*)
						bind_data.url_filter = ConvertSQLWildcardsToCDX(original_pattern);
						fprintf(stderr, "[DEBUG] LIKE URL filter converted pattern: '%s'\n", bind_data.url_filter.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
		}

		// Check if this is a comparison expression
		if (filter->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
			continue;
		}
		if (filter->type != ExpressionType::COMPARE_EQUAL &&
		    filter->type != ExpressionType::COMPARE_NOTEQUAL) {
			continue;
		}

		auto &comparison = filter->Cast<BoundComparisonExpression>();

		// Check if left side is a bound column reference
		if (comparison.left->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		auto &col_ref = comparison.left->Cast<BoundColumnRefExpression>();

		// Check if right side is a constant value
		if (comparison.right->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			continue;
		}
		auto &constant = comparison.right->Cast<BoundConstantExpression>();

		// Get the column binding
		idx_t table_index = col_ref.binding.table_index;

		// Get the column name from the expression itself (this is the most reliable way)
		string column_name = col_ref.GetName();

		// Validate table index matches our table
		if (table_index != get.table_index) {
			continue;
		}

		// Handle URL filtering (special case - uses url parameter)
		if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			string original_pattern = constant.value.ToString();
			fprintf(stderr, "[DEBUG] URL filter original pattern: '%s'\n", original_pattern.c_str());
			// Convert SQL LIKE wildcards (%) to CDX API wildcards (*)
			bind_data.url_filter = ConvertSQLWildcardsToCDX(original_pattern);
			fprintf(stderr, "[DEBUG] URL filter converted pattern: '%s'\n", bind_data.url_filter.c_str());
			filters_to_remove.push_back(i);
		}
		// Handle crawl_id filtering (sets the index_name to use)
		else if (column_name == "crawl_id" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				bind_data.index_name = constant.value.ToString();
				fprintf(stderr, "[DEBUG] crawl_id filter set index_name to: %s\n", bind_data.index_name.c_str());
				filters_to_remove.push_back(i);
			}
		}
		// Handle statuscode filtering (uses CDX filter parameter)
		// CDX API syntax: filter==statuscode:200 means &filter==statuscode:200
		// (one = for parameter, one = for exact match operator)
		else if (column_name == "statuscode" &&
		         (constant.value.type().id() == LogicalTypeId::INTEGER ||
		          constant.value.type().id() == LogicalTypeId::BIGINT)) {
			string op = (filter->type == ExpressionType::COMPARE_EQUAL) ? "=" : "!";
			string filter_str = op + "statuscode:" + to_string(constant.value.GetValue<int32_t>());
			bind_data.cdx_filters.push_back(filter_str);
			filters_to_remove.push_back(i);
		}
		// Handle mimetype filtering (uses CDX filter parameter)
		else if (column_name == "mimetype" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			string op = (filter->type == ExpressionType::COMPARE_EQUAL) ? "=" : "!";
			string filter_str = op + "mime:" + constant.value.ToString();
			bind_data.cdx_filters.push_back(filter_str);
			filters_to_remove.push_back(i);
		}
	}

	// Remove filters that we've pushed down (in reverse order to maintain indices)
	for (idx_t i = filters_to_remove.size(); i > 0; i--) {
		filters.erase(filters.begin() + filters_to_remove[i - 1]);
	}
}

// Cardinality function to provide row count estimates to DuckDB optimizer
static unique_ptr<NodeStatistics> CommonCrawlCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<CommonCrawlBindData>();
	// Return the max results as an estimate - this helps DuckDB optimize the query plan
	return make_uniq<NodeStatistics>(bind_data.max_results);
}

// ========================================
// OPTIMIZER FOR LIMIT PUSHDOWN
// ========================================

// Optimizer function to push down LIMIT to common_crawl_index function
void OptimizeCommonCrawlLimitPushdown(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection operators to find the GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeCommonCrawlLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "common_crawl_index") {
			OptimizeCommonCrawlLimitPushdown(op->children[0]);
			return;
		}

		// Only push down constant limits (not expressions)
		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			// not a constant or unset limit
			OptimizeCommonCrawlLimitPushdown(op->children[0]);
			return;
		}

		// Extract limit value and store in bind_data
		auto &bind_data = get.bind_data->Cast<CommonCrawlBindData>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			auto limit_value = limit.limit_val.GetConstantValue();
			// For common_crawl with multiple crawl_ids, divide limit across crawl_ids
			if (!bind_data.crawl_ids.empty()) {
				limit_value = (limit_value + bind_data.crawl_ids.size() - 1) / bind_data.crawl_ids.size();
				fprintf(stderr, "[DEBUG] LIMIT pushdown: max_results set to %lu (divided across %lu crawl_ids)\n",
				        (unsigned long)limit_value, (unsigned long)bind_data.crawl_ids.size());
			} else {
				fprintf(stderr, "[DEBUG] LIMIT pushdown: max_results set to %lu\n",
				        (unsigned long)limit_value);
			}
			bind_data.max_results = limit_value;

			// Remove the LIMIT node from the plan since we've pushed it down
			op = std::move(op->children[0]);
			return;
		}
	}

	// Recurse into children
	for (auto &child : op->children) {
		OptimizeCommonCrawlLimitPushdown(child);
	}
}

// ========================================
// REGISTRATION
// ========================================

void RegisterCommonCrawlFunction(ExtensionLoader &loader) {
	// Register the common_crawl_index table function
	// Usage: SELECT * FROM common_crawl_index() WHERE crawl_id = 'CC-MAIN-2025-43' LIMIT 10
	// Usage with max_results: SELECT * FROM common_crawl_index(max_results := 500) WHERE crawl_id = 'CC-MAIN-2025-43'
	// - crawl_id filtering is done via WHERE clause (defaults to latest if not specified)
	// - URL filtering: WHERE url LIKE '*.example.com/*'
	// - Optional max_results parameter controls CDX API result size (default: 100)
	TableFunctionSet common_crawl_set("common_crawl_index");

	auto func = TableFunction({},
	                          CommonCrawlScan, CommonCrawlBind, CommonCrawlInitGlobal);
	func.cardinality = CommonCrawlCardinality;
	func.pushdown_complex_filter = CommonCrawlPushdownComplexFilter;
	func.projection_pushdown = true;

	// Add named parameter
	func.named_parameters["max_results"] = LogicalType::BIGINT;

	common_crawl_set.AddFunction(func);

	loader.RegisterFunction(common_crawl_set);
}

} // namespace duckdb
