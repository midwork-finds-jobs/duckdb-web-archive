#include "common_crawl_utils.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

// ========================================
// BIND DATA AND STATE
// ========================================

// Structure to hold bind data for internet_archive table function
struct InternetArchiveBindData : public TableFunctionData {
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> fields_needed;
	bool fetch_response;
	bool cdx_url_only;  // True if only cdx_url column is selected (skip network request)
	string url_filter;
	string match_type; // exact, prefix, host, domain
	vector<string> cdx_filters; // filter=field:regex
	string from_date; // YYYYMMDDhhmmss
	string to_date;   // YYYYMMDDhhmmss
	idx_t max_results; // Default limit
	string collapse;  // collapse parameter (e.g., "urlkey", "timestamp:8")
	string cdx_url;   // The constructed CDX API URL (populated after query)
	bool fast_latest; // Use fastLatest=true for efficient ORDER BY timestamp DESC
	bool order_desc;  // ORDER BY timestamp DESC detected

	InternetArchiveBindData() : fetch_response(false), cdx_url_only(false), url_filter("*"), match_type("exact"), max_results(100), collapse(""), cdx_url(""), fast_latest(false), order_desc(false) {}
};

// Structure to hold global state for internet_archive table function
struct InternetArchiveGlobalState : public GlobalTableFunctionState {
	vector<ArchiveOrgRecord> records;
	idx_t current_position;
	vector<column_t> column_ids;

	InternetArchiveGlobalState() : current_position(0) {}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded
	}
};

// ========================================
// CDX URL BUILDING
// ========================================

// Helper function to build Internet Archive CDX URL (without making request)
static string BuildArchiveOrgCDXUrl(const string &url_pattern, const string &match_type,
                                     const vector<string> &fields_needed, const vector<string> &cdx_filters,
                                     const string &from_date, const string &to_date, idx_t max_results,
                                     const string &collapse, bool fast_latest) {
	// Construct field list for &fl= parameter from fields_needed
	std::set<string> needed_set(fields_needed.begin(), fields_needed.end());

	string field_list;
	// Order matters for parsing - keep consistent order
	vector<string> ordered_fields = {"urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"};
	for (const auto &f : ordered_fields) {
		if (needed_set.count(f)) {
			if (!field_list.empty()) field_list += ",";
			field_list += f;
		}
	}

	// Construct the CDX API URL (use CSV format - space delimited, fields in fl order)
	string cdx_url = "https://web.archive.org/cdx/search/cdx?url=" + url_pattern +
	                 "&output=csv&fl=" + field_list;

	// Add matchType if not exact (default)
	if (match_type != "exact") {
		cdx_url += "&matchType=" + match_type;
	}

	// Add date range filters
	if (!from_date.empty()) {
		cdx_url += "&from=" + from_date;
	}
	if (!to_date.empty()) {
		cdx_url += "&to=" + to_date;
	}

	// Add limit (negative for fastLatest to get latest results)
	if (fast_latest) {
		cdx_url += "&fastLatest=true&limit=-" + to_string(max_results);
	} else {
		cdx_url += "&limit=" + to_string(max_results);
	}

	// Add filter parameters
	for (const auto &filter : cdx_filters) {
		cdx_url += "&filter=" + filter;
	}

	// Add collapse parameter if specified
	if (!collapse.empty()) {
		cdx_url += "&collapse=" + collapse;
	}

	return cdx_url;
}

// ========================================
// CDX API QUERY
// ========================================

// Helper function to query Internet Archive CDX API
static vector<ArchiveOrgRecord> QueryArchiveOrgCDX(ClientContext &context, const string &url_pattern,
                                                     const string &match_type, const vector<string> &fields_needed,
                                                     const vector<string> &cdx_filters, const string &from_date,
                                                     const string &to_date, idx_t max_results, const string &collapse,
                                                     bool fast_latest, string &out_cdx_url) {
	fprintf(stderr, "[DEBUG +%.0fms] QueryArchiveOrgCDX started\n", ElapsedMs());
	vector<ArchiveOrgRecord> records;

	// Build the CDX URL
	string cdx_url = BuildArchiveOrgCDXUrl(url_pattern, match_type, fields_needed, cdx_filters,
	                                        from_date, to_date, max_results, collapse, fast_latest);

	// Construct field list for parsing (same logic as BuildArchiveOrgCDXUrl)
	std::set<string> needed_set(fields_needed.begin(), fields_needed.end());
	string field_list;
	vector<string> ordered_fields = {"urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"};
	for (const auto &f : ordered_fields) {
		if (needed_set.count(f)) {
			if (!field_list.empty()) field_list += ",";
			field_list += f;
		}
	}
	fprintf(stderr, "[DEBUG] Internet Archive CDX fields: %s\n", field_list.c_str());
	fprintf(stderr, "[CDX URL +%.0fms] %s\n", ElapsedMs(), cdx_url.c_str());

	// Store the CDX URL for output
	out_cdx_url = cdx_url;

	try {
		auto &fs = FileSystem::GetFileSystem(context);
		auto file_handle = fs.OpenFile(cdx_url, FileFlags::FILE_FLAGS_READ);

		// Read the response
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

		// Sanitize UTF-8
		response_data = SanitizeUTF8(response_data);

		// Build list of fields we're requesting (in order)
		vector<string> fields_in_order;
		for (const auto &f : ordered_fields) {
			if (needed_set.count(f)) {
				fields_in_order.push_back(f);
			}
		}

		// Parse tab-delimited CSV (fields in same order as fl parameter)
		std::istringstream stream(response_data);
		string line;
		int line_count = 0;

		while (std::getline(stream, line)) {
			if (line.empty()) {
				continue;
			}
			line_count++;

			// Split by space (Internet Archive CDX uses space delimiter for CSV)
			vector<string> values;
			std::istringstream line_stream(line);
			string value;
			while (line_stream >> value) {
				values.push_back(value);
			}

			if (values.size() < fields_in_order.size()) {
				continue;  // Skip malformed lines
			}

			// Parse data row using known field order
			ArchiveOrgRecord record;
			for (size_t i = 0; i < fields_in_order.size(); i++) {
				const string &field = fields_in_order[i];
				const string &val = values[i];

				if (field == "urlkey") {
					record.urlkey = val;
				} else if (field == "timestamp") {
					record.timestamp = val;
				} else if (field == "original") {
					record.original = val;
				} else if (field == "mimetype") {
					record.mime_type = val;
				} else if (field == "statuscode") {
					record.status_code = val.empty() || val == "-" ? 0 : std::stoi(val);
				} else if (field == "digest") {
					record.digest = val;
				} else if (field == "length") {
					record.length = val.empty() || val == "-" ? 0 : std::stoll(val);
				}
			}
			records.push_back(record);
		}
		fprintf(stderr, "[DEBUG] Parsed %d CSV lines, got %lu records\n", line_count, (unsigned long)records.size());

	} catch (std::exception &ex) {
		throw IOException("Error querying Internet Archive CDX API: " + string(ex.what()));
	}

	return records;
}

// ========================================
// ARCHIVED PAGE FETCHING
// ========================================

// Helper function to fetch archived page from Internet Archive
static string FetchArchivedPage(ClientContext &context, const ArchiveOrgRecord &record) {
	if (record.timestamp.empty() || record.original.empty()) {
		return "[Error: Missing timestamp or URL]";
	}

	try {
		// Construct the download URL with id_ suffix to get raw content
		string download_url = "https://web.archive.org/web/" + record.timestamp + "id_/" + record.original;
		fprintf(stderr, "[DEBUG] Fetching: %s\n", download_url.c_str());

		auto &fs = FileSystem::GetFileSystem(context);
		auto file_handle = fs.OpenFile(download_url, FileFlags::FILE_FLAGS_READ);

		// Read the response
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

		return response_data;

	} catch (Exception &ex) {
		return "[Error fetching archived page: " + string(ex.what()) + "]";
	} catch (std::exception &ex) {
		return "[Error fetching archived page: " + string(ex.what()) + "]";
	}
}

// ========================================
// TABLE FUNCTION IMPLEMENTATION
// ========================================

// Bind function for internet_archive table function
static unique_ptr<FunctionData> InternetArchiveBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	g_start_time = std::chrono::steady_clock::now();
	fprintf(stderr, "[DEBUG +%.0fms] InternetArchiveBind called\n", ElapsedMs());

	auto bind_data = make_uniq<InternetArchiveBindData>();

	// Handle named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "max_results") {
			if (kv.second.type().id() != LogicalTypeId::BIGINT) {
				throw BinderException("internet_archive max_results parameter must be an integer");
			}
			bind_data->max_results = kv.second.GetValue<int64_t>();
			fprintf(stderr, "[DEBUG] CDX API max_results set to: %lu\n", (unsigned long)bind_data->max_results);
		} else if (kv.first == "collapse") {
			if (kv.second.type().id() != LogicalTypeId::VARCHAR) {
				throw BinderException("internet_archive collapse parameter must be a string");
			}
			bind_data->collapse = kv.second.GetValue<string>();
			fprintf(stderr, "[DEBUG] CDX API collapse set to: %s\n", bind_data->collapse.c_str());
		} else {
			throw BinderException("Unknown parameter '%s' for internet_archive", kv.first.c_str());
		}
	}

	// Define output columns
	names.push_back("url");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("original");

	names.push_back("timestamp");
	return_types.push_back(LogicalType::TIMESTAMP_TZ);
	bind_data->fields_needed.push_back("timestamp");

	names.push_back("urlkey");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("urlkey");

	names.push_back("mime_type");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("mimetype");

	names.push_back("status_code");
	return_types.push_back(LogicalType::INTEGER);
	bind_data->fields_needed.push_back("statuscode");

	names.push_back("digest");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("digest");

	names.push_back("length");
	return_types.push_back(LogicalType::BIGINT);
	bind_data->fields_needed.push_back("length");

	// Add response column (BLOB for raw HTTP body)
	names.push_back("response");
	return_types.push_back(LogicalType::BLOB);

	// Add cdx_url column (the CDX API URL used for the query)
	names.push_back("cdx_url");
	return_types.push_back(LogicalType::VARCHAR);

	// Don't set fetch_response here - will be determined by projection pushdown
	bind_data->fetch_response = false;
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

// Init global state function for internet_archive
static unique_ptr<GlobalTableFunctionState> InternetArchiveInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	fprintf(stderr, "[DEBUG +%.0fms] InternetArchiveInitGlobal called\n", ElapsedMs());
	auto &bind_data = const_cast<InternetArchiveBindData&>(input.bind_data->Cast<InternetArchiveBindData>());
	auto state = make_uniq<InternetArchiveGlobalState>();

	// Store projected columns
	state->column_ids = input.column_ids;

	// Rebuild fields_needed based on projection pushdown
	// Map column names to CDX API field names
	bind_data.fields_needed.clear();
	for (auto &col_id : input.column_ids) {
		if (col_id < bind_data.column_names.size()) {
			string col_name = bind_data.column_names[col_id];
			fprintf(stderr, "[DEBUG] Projected column: %s\n", col_name.c_str());

			if (col_name == "url") {
				bind_data.fields_needed.push_back("original");
			} else if (col_name == "timestamp") {
				bind_data.fields_needed.push_back("timestamp");
			} else if (col_name == "urlkey") {
				bind_data.fields_needed.push_back("urlkey");
			} else if (col_name == "mime_type") {
				bind_data.fields_needed.push_back("mimetype");
			} else if (col_name == "status_code") {
				bind_data.fields_needed.push_back("statuscode");
			} else if (col_name == "digest") {
				bind_data.fields_needed.push_back("digest");
			} else if (col_name == "length") {
				bind_data.fields_needed.push_back("length");
			} else if (col_name == "response") {
				bind_data.fetch_response = true;
				fprintf(stderr, "[DEBUG] Will fetch response bodies\n");
			} else if (col_name == "cdx_url") {
				// cdx_url doesn't need any CDX fields
			}
		}
	}

	// Check if only cdx_url is selected (fields_needed is empty and no response)
	bind_data.cdx_url_only = bind_data.fields_needed.empty() && !bind_data.fetch_response;

	if (bind_data.cdx_url_only) {
		// Only cdx_url is selected - build URL without network request
		fprintf(stderr, "[DEBUG] Only cdx_url selected - skipping network request\n");
		bind_data.cdx_url = BuildArchiveOrgCDXUrl(bind_data.url_filter, bind_data.match_type,
		                                           bind_data.fields_needed, bind_data.cdx_filters,
		                                           bind_data.from_date, bind_data.to_date, bind_data.max_results,
		                                           bind_data.collapse, bind_data.fast_latest);
		fprintf(stderr, "[CDX URL +%.0fms] %s\n", ElapsedMs(), bind_data.cdx_url.c_str());

		// Create a single dummy record so we return one row with the cdx_url
		ArchiveOrgRecord dummy;
		state->records.push_back(dummy);
	} else {
		// Query Internet Archive CDX API
		state->records = QueryArchiveOrgCDX(context, bind_data.url_filter, bind_data.match_type,
		                                     bind_data.fields_needed, bind_data.cdx_filters,
		                                     bind_data.from_date, bind_data.to_date, bind_data.max_results,
		                                     bind_data.collapse, bind_data.fast_latest, bind_data.cdx_url);
	}

	fprintf(stderr, "[DEBUG] QueryArchiveOrgCDX returned %lu records\n", (unsigned long)state->records.size());

	return std::move(state);
}

// Scan function for internet_archive table function
static void InternetArchiveScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<InternetArchiveBindData>();
	auto &gstate = data.global_state->Cast<InternetArchiveGlobalState>();

	// Pre-fetch responses in parallel for this chunk if needed
	std::vector<string> response_bodies;
	idx_t chunk_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, gstate.records.size() - gstate.current_position);

	if (bind_data.fetch_response && chunk_size > 0) {
		fprintf(stderr, "[DEBUG] Pre-fetching %lu archived pages in parallel\n", (unsigned long)chunk_size);
		std::vector<std::future<string>> response_futures;
		response_futures.reserve(chunk_size);

		// Launch parallel fetches
		for (idx_t i = 0; i < chunk_size; i++) {
			auto &record = gstate.records[gstate.current_position + i];
			response_futures.push_back(std::async(std::launch::async, [&context, record]() {
				return FetchArchivedPage(context, record);
			}));
		}

		// Collect results
		response_bodies.reserve(chunk_size);
		for (auto &future : response_futures) {
			response_bodies.push_back(future.get());
		}
		fprintf(stderr, "[DEBUG] All %lu archived pages fetched\n", (unsigned long)chunk_size);
	}

	idx_t output_offset = 0;
	while (gstate.current_position < gstate.records.size() && output_offset < STANDARD_VECTOR_SIZE) {
		auto &record = gstate.records[gstate.current_position];

		// Process each projected column
		for (idx_t proj_idx = 0; proj_idx < gstate.column_ids.size(); proj_idx++) {
			auto col_id = gstate.column_ids[proj_idx];
			string col_name = bind_data.column_names[col_id];

			try {
				if (col_name == "url") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.original));
				} else if (col_name == "timestamp") {
					auto data_ptr = FlatVector::GetData<timestamp_t>(output.data[proj_idx]);
					data_ptr[output_offset] = ParseCDXTimestamp(record.timestamp);
				} else if (col_name == "urlkey") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.urlkey));
				} else if (col_name == "mime_type") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.mime_type));
				} else if (col_name == "status_code") {
					auto data_ptr = FlatVector::GetData<int32_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.status_code;
				} else if (col_name == "digest") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.digest));
				} else if (col_name == "length") {
					auto data_ptr = FlatVector::GetData<int64_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.length;
				} else if (col_name == "response") {
					if (bind_data.fetch_response && !response_bodies.empty()) {
						string &body = response_bodies[output_offset];
						auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
						data_ptr[output_offset] = StringVector::AddStringOrBlob(output.data[proj_idx], body);
					} else {
						FlatVector::SetNull(output.data[proj_idx], output_offset, true);
					}
				} else if (col_name == "cdx_url") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], bind_data.cdx_url);
				}
			} catch (const std::exception &ex) {
				fprintf(stderr, "[ERROR] Failed to process column %s: %s\n", col_name.c_str(), ex.what());
			}
		}

		output_offset++;
		gstate.current_position++;
	}

	output.SetCardinality(output_offset);
}

// ========================================
// FILTER PUSHDOWN
// ========================================

// Filter pushdown for internet_archive
static void InternetArchivePushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                                   vector<unique_ptr<Expression>> &filters) {
	fprintf(stderr, "[DEBUG +%.0fms] InternetArchivePushdownComplexFilter called with %lu filters\n", ElapsedMs(), (unsigned long)filters.size());
	auto &bind_data = bind_data_p->Cast<InternetArchiveBindData>();

	// Build column map
	std::unordered_map<string, idx_t> column_map;
	for (idx_t i = 0; i < bind_data.column_names.size(); i++) {
		column_map[bind_data.column_names[i]] = i;
	}

	vector<idx_t> filters_to_remove;

	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];

		// Handle LIKE/CONTAINS for URL filtering
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func = filter->Cast<BoundFunctionExpression>();

			// Handle LIKE for url column
			if ((func.function.name == "like" || func.function.name == "~~") &&
			    func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();

				if (col_ref.GetName() == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					bind_data.url_filter = constant.value.ToString();

					// Detect matchType from pattern
					if (bind_data.url_filter.find("*") == 0) {
						bind_data.match_type = "domain";
						bind_data.url_filter = bind_data.url_filter.substr(1); // Remove leading *
					} else if (bind_data.url_filter.find("/*") != string::npos) {
						bind_data.match_type = "prefix";
						bind_data.url_filter = bind_data.url_filter.substr(0, bind_data.url_filter.find("/*"));
					}

					fprintf(stderr, "[DEBUG] URL filter: %s, matchType: %s\n",
					        bind_data.url_filter.c_str(), bind_data.match_type.c_str());
					filters_to_remove.push_back(i);
					continue;
				}

				// Handle LIKE for urlkey column: urlkey LIKE '%apply' -> filter=urlkey:.*apply$
				if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string like_pattern = constant.value.ToString();
					string regex_pattern = LikeToRegex(like_pattern);
					string filter_str = "urlkey:" + regex_pattern;
					bind_data.cdx_filters.push_back(filter_str);
					fprintf(stderr, "[DEBUG +%.0fms] urlkey LIKE: %s -> %s\n", ElapsedMs(), like_pattern.c_str(), filter_str.c_str());
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle suffix(urlkey, 'pattern') -> filter=urlkey:.*pattern$
			// DuckDB optimizes LIKE '%x' to suffix()
			if (func.function.name == "suffix" &&
			    func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();

				if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string suffix_val = constant.value.ToString();
					string filter_str = "urlkey:.*" + suffix_val + "$";
					bind_data.cdx_filters.push_back(filter_str);
					fprintf(stderr, "[DEBUG +%.0fms] urlkey suffix: %s\n", ElapsedMs(), filter_str.c_str());
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle prefix(urlkey, 'pattern') -> filter=urlkey:^pattern.*
			// DuckDB optimizes LIKE 'x%' to prefix()
			if (func.function.name == "prefix" &&
			    func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();

				if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string prefix_val = constant.value.ToString();
					string filter_str = "urlkey:^" + prefix_val + ".*";
					bind_data.cdx_filters.push_back(filter_str);
					fprintf(stderr, "[DEBUG +%.0fms] urlkey prefix: %s\n", ElapsedMs(), filter_str.c_str());
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle contains(urlkey, 'pattern') -> filter=urlkey:.*pattern.*
			// DuckDB optimizes LIKE '%x%' to contains()
			if (func.function.name == "contains" &&
			    func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();

				if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string contains_val = constant.value.ToString();
					// Escape regex special chars in the search string
					string escaped;
					for (char c : contains_val) {
						if (c == '.' || c == '(' || c == ')' || c == '[' || c == ']' ||
						    c == '{' || c == '}' || c == '+' || c == '?' || c == '^' ||
						    c == '$' || c == '|' || c == '\\' || c == '*') {
							escaped += '\\';
						}
						escaped += c;
					}
					string filter_str = "urlkey:.*" + escaped + ".*";
					bind_data.cdx_filters.push_back(filter_str);
					fprintf(stderr, "[DEBUG +%.0fms] urlkey contains: %s\n", ElapsedMs(), filter_str.c_str());
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle regexp_matches for urlkey: urlkey ~ 'regex'
			if ((func.function.name == "regexp_matches" || func.function.name == "~") &&
			    func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();

				if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string regex_pattern = constant.value.ToString();
					string filter_str = "urlkey:" + regex_pattern;
					bind_data.cdx_filters.push_back(filter_str);
					fprintf(stderr, "[DEBUG +%.0fms] urlkey regex: %s\n", ElapsedMs(), filter_str.c_str());
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle regexp_full_match for urlkey: SIMILAR TO converts to this
			// SIMILAR TO uses SQL regex: % = .*, _ = ., and it's a full match (anchored)
			if (func.function.name == "regexp_full_match" &&
			    func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();

				if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					// Convert SQL regex (%, _) to standard regex (already a full match)
					string sql_regex = constant.value.ToString();
					string regex_pattern = "^";
					for (char c : sql_regex) {
						if (c == '%') {
							regex_pattern += ".*";
						} else if (c == '_') {
							regex_pattern += ".";
						} else {
							regex_pattern += c;
						}
					}
					regex_pattern += "$";
					string filter_str = "urlkey:" + regex_pattern;
					bind_data.cdx_filters.push_back(filter_str);
					fprintf(stderr, "[DEBUG +%.0fms] urlkey SIMILAR TO: %s -> %s\n", ElapsedMs(), sql_regex.c_str(), filter_str.c_str());
					filters_to_remove.push_back(i);
					continue;
				}
			}

		}

		// Handle NOT (OPERATOR_NOT) for urlkey: NOT regexp_matches() or NOT LIKE
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
		    filter->type == ExpressionType::OPERATOR_NOT) {
			auto &op = filter->Cast<BoundOperatorExpression>();
			if (op.children.size() >= 1 &&
			    op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {

				auto &inner_func = op.children[0]->Cast<BoundFunctionExpression>();

				// NOT regexp_matches(urlkey, 'regex')
				if ((inner_func.function.name == "regexp_matches" || inner_func.function.name == "~") &&
				    inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string regex_pattern = constant.value.ToString();
						string filter_str = "!urlkey:" + regex_pattern;
						bind_data.cdx_filters.push_back(filter_str);
						fprintf(stderr, "[DEBUG +%.0fms] urlkey NOT regex: %s\n", ElapsedMs(), filter_str.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT (urlkey LIKE '%pattern') -> !urlkey:.*pattern$
				if ((inner_func.function.name == "like" || inner_func.function.name == "~~") &&
				    inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string like_pattern = constant.value.ToString();
						string regex_pattern = LikeToRegex(like_pattern);
						string filter_str = "!urlkey:" + regex_pattern;
						bind_data.cdx_filters.push_back(filter_str);
						fprintf(stderr, "[DEBUG +%.0fms] urlkey NOT LIKE: %s -> %s\n", ElapsedMs(), like_pattern.c_str(), filter_str.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT suffix(urlkey, 'pattern') -> !urlkey:.*pattern$
				if (inner_func.function.name == "suffix" &&
				    inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string suffix_val = constant.value.ToString();
						string filter_str = "!urlkey:.*" + suffix_val + "$";
						bind_data.cdx_filters.push_back(filter_str);
						fprintf(stderr, "[DEBUG +%.0fms] urlkey NOT suffix: %s\n", ElapsedMs(), filter_str.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT prefix(urlkey, 'pattern') -> !urlkey:^pattern.*
				if (inner_func.function.name == "prefix" &&
				    inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string prefix_val = constant.value.ToString();
						string filter_str = "!urlkey:^" + prefix_val + ".*";
						bind_data.cdx_filters.push_back(filter_str);
						fprintf(stderr, "[DEBUG +%.0fms] urlkey NOT prefix: %s\n", ElapsedMs(), filter_str.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT SIMILAR TO -> NOT regexp_full_match
				if (inner_func.function.name == "regexp_full_match" &&
				    inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string sql_regex = constant.value.ToString();
						string regex_pattern = "^";
						for (char c : sql_regex) {
							if (c == '%') {
								regex_pattern += ".*";
							} else if (c == '_') {
								regex_pattern += ".";
							} else {
								regex_pattern += c;
							}
						}
						regex_pattern += "$";
						string filter_str = "!urlkey:" + regex_pattern;
						bind_data.cdx_filters.push_back(filter_str);
						fprintf(stderr, "[DEBUG +%.0fms] urlkey NOT SIMILAR TO: %s -> %s\n", ElapsedMs(), sql_regex.c_str(), filter_str.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT contains(urlkey, 'pattern') -> !urlkey:.*pattern.*
				if (inner_func.function.name == "contains" &&
				    inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string contains_val = constant.value.ToString();
						// Escape regex special chars
						string escaped;
						for (char c : contains_val) {
							if (c == '.' || c == '(' || c == ')' || c == '[' || c == ']' ||
							    c == '{' || c == '}' || c == '+' || c == '?' || c == '^' ||
							    c == '$' || c == '|' || c == '\\' || c == '*') {
								escaped += '\\';
							}
							escaped += c;
						}
						string filter_str = "!urlkey:.*" + escaped + ".*";
						bind_data.cdx_filters.push_back(filter_str);
						fprintf(stderr, "[DEBUG +%.0fms] urlkey NOT contains: %s\n", ElapsedMs(), filter_str.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
		}

		// Handle BETWEEN expressions (timestamp BETWEEN x AND y)
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
			auto &between = filter->Cast<BoundBetweenExpression>();
			if (between.input->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    between.lower->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
			    between.upper->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				auto &col_ref = between.input->Cast<BoundColumnRefExpression>();
				if (col_ref.GetName() == "timestamp") {
					auto &lower_const = between.lower->Cast<BoundConstantExpression>();
					auto &upper_const = between.upper->Cast<BoundConstantExpression>();

					bind_data.from_date = ToCdxTimestamp(lower_const.value.ToString());
					bind_data.to_date = ToCdxTimestamp(upper_const.value.ToString());
					fprintf(stderr, "[DEBUG +%.0fms] BETWEEN from=%s to=%s\n", ElapsedMs(),
					        bind_data.from_date.c_str(), bind_data.to_date.c_str());

					filters_to_remove.push_back(i);
					continue;
				}
			}
		}

		// Handle comparison filters
		if (filter->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
			continue;
		}

		auto &comparison = filter->Cast<BoundComparisonExpression>();
		if (comparison.left->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    comparison.right->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			continue;
		}

		auto &col_ref = comparison.left->Cast<BoundColumnRefExpression>();
		auto &constant = comparison.right->Cast<BoundConstantExpression>();
		string column_name = col_ref.GetName();

		// Handle URL filtering via equality/LIKE
		if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				bind_data.url_filter = constant.value.ToString();
				bind_data.match_type = "exact";
				fprintf(stderr, "[DEBUG] URL filter (exact): %s\n", bind_data.url_filter.c_str());
				filters_to_remove.push_back(i);
			}
		}
		// Handle status_code filtering
		if (column_name == "status_code" &&
		    (constant.value.type().id() == LogicalTypeId::INTEGER ||
		     constant.value.type().id() == LogicalTypeId::BIGINT)) {
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				string filter_str = "statuscode:" + to_string(constant.value.GetValue<int32_t>());
				bind_data.cdx_filters.push_back(filter_str);
				filters_to_remove.push_back(i);
			} else if (filter->type == ExpressionType::COMPARE_NOTEQUAL) {
				string filter_str = "!statuscode:" + to_string(constant.value.GetValue<int32_t>());
				bind_data.cdx_filters.push_back(filter_str);
				filters_to_remove.push_back(i);
			}
		}
		// Handle mime_type filtering
		else if (column_name == "mime_type" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				string filter_str = "mimetype:" + constant.value.ToString();
				bind_data.cdx_filters.push_back(filter_str);
				filters_to_remove.push_back(i);
			} else if (filter->type == ExpressionType::COMPARE_NOTEQUAL) {
				string filter_str = "!mimetype:" + constant.value.ToString();
				bind_data.cdx_filters.push_back(filter_str);
				filters_to_remove.push_back(i);
			}
		}
		// Handle timestamp filtering (from/to date range)
		else if (column_name == "timestamp" &&
		         (constant.value.type().id() == LogicalTypeId::TIMESTAMP ||
		          constant.value.type().id() == LogicalTypeId::TIMESTAMP_TZ ||
		          constant.value.type().id() == LogicalTypeId::DATE ||
		          constant.value.type().id() == LogicalTypeId::VARCHAR)) {

			string cdx_timestamp = ToCdxTimestamp(constant.value.ToString());

			if (filter->type == ExpressionType::COMPARE_GREATERTHAN ||
			    filter->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				bind_data.from_date = cdx_timestamp;
				fprintf(stderr, "[DEBUG +%.0fms] Timestamp from: %s\n", ElapsedMs(), cdx_timestamp.c_str());
				filters_to_remove.push_back(i);
			} else if (filter->type == ExpressionType::COMPARE_LESSTHAN ||
			           filter->type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
				bind_data.to_date = cdx_timestamp;
				fprintf(stderr, "[DEBUG +%.0fms] Timestamp to: %s\n", ElapsedMs(), cdx_timestamp.c_str());
				filters_to_remove.push_back(i);
			}
		}
	}

	// Remove pushed down filters
	for (idx_t i = filters_to_remove.size(); i > 0; i--) {
		filters.erase(filters.begin() + filters_to_remove[i - 1]);
	}
}

// Cardinality function for internet_archive
static unique_ptr<NodeStatistics> InternetArchiveCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<InternetArchiveBindData>();
	return make_uniq<NodeStatistics>(bind_data.max_results);
}

// ========================================
// OPTIMIZER FOR LIMIT PUSHDOWN
// ========================================

// Helper function to check if TOP_N orders by timestamp DESC
static bool IsTimestampDescTopN(LogicalTopN &top_n, const InternetArchiveBindData &bind_data) {
	if (top_n.orders.empty()) {
		return false;
	}

	// Check if the first (and ideally only) order is timestamp DESC
	auto &first_order = top_n.orders[0];
	fprintf(stderr, "[DEBUG] TOP_N order type: %s\n",
	        first_order.type == OrderType::DESCENDING ? "DESC" : "ASC");

	if (first_order.type != OrderType::DESCENDING) {
		return false;
	}

	// Check if ordering by the timestamp column
	auto expr_class = first_order.expression->GetExpressionClass();
	fprintf(stderr, "[DEBUG] TOP_N expression class: %d\n", (int)expr_class);

	if (expr_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = first_order.expression->Cast<BoundColumnRefExpression>();
		fprintf(stderr, "[DEBUG] TOP_N column name: '%s', alias: '%s'\n",
		        col_ref.GetName().c_str(), col_ref.alias.c_str());

		// Check both GetName() and alias - DuckDB might use either
		if (col_ref.GetName() == "timestamp" || col_ref.alias == "timestamp") {
			return true;
		}

		// Also check by column binding - timestamp is column index 1 in our schema
		// (url=0, timestamp=1, urlkey=2, mime_type=3, status_code=4, digest=5, length=6, response=7, cdx_url=8)
		if (col_ref.binding.column_index == 1) {
			fprintf(stderr, "[DEBUG] TOP_N matched timestamp by column index\n");
			return true;
		}
	}

	return false;
}

// Optimizer function to push down LIMIT to internet_archive function
void OptimizeInternetArchiveLimitPushdown(unique_ptr<LogicalOperator> &op) {
	// Handle TOP_N (ORDER BY + LIMIT combined)
	if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &top_n = op->Cast<LogicalTopN>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection operators to find GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeInternetArchiveLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "internet_archive") {
			OptimizeInternetArchiveLimitPushdown(op->children[0]);
			return;
		}

		auto &bind_data = get.bind_data->Cast<InternetArchiveBindData>();

		// Check if ORDER BY timestamp DESC
		if (IsTimestampDescTopN(top_n, bind_data)) {
			bind_data.max_results = top_n.limit;
			bind_data.fast_latest = true;
			bind_data.order_desc = true;
			fprintf(stderr, "[DEBUG] TOP_N timestamp DESC pushdown: fastLatest=true, limit=-%lu\n",
			        (unsigned long)bind_data.max_results);

			// Remove TOP_N - API returns results in DESC order already
			op = std::move(op->children[0]);
			return;
		} else {
			// Regular TOP_N - just push down the limit
			bind_data.max_results = top_n.limit;
			fprintf(stderr, "[DEBUG] TOP_N pushdown: max_results set to %lu\n",
			        (unsigned long)bind_data.max_results);
			// Keep TOP_N in plan for non-DESC ordering
		}
	}

	// Handle plain LIMIT (no ORDER BY)
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection operators to find GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeInternetArchiveLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "internet_archive") {
			OptimizeInternetArchiveLimitPushdown(op->children[0]);
			return;
		}

		// Only push down constant limits (not expressions)
		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			// not a constant or unset limit
			OptimizeInternetArchiveLimitPushdown(op->children[0]);
			return;
		}

		// Extract limit value and store in bind_data
		auto &bind_data = get.bind_data->Cast<InternetArchiveBindData>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			bind_data.max_results = limit.limit_val.GetConstantValue();
			fprintf(stderr, "[DEBUG] LIMIT pushdown: max_results set to %lu\n",
			        (unsigned long)bind_data.max_results);

			// Remove the LIMIT node from the plan since we've pushed it down
			op = std::move(op->children[0]);
			return;
		}
	}

	// Recurse into children
	for (auto &child : op->children) {
		OptimizeInternetArchiveLimitPushdown(child);
	}
}

// ========================================
// REGISTRATION
// ========================================

void RegisterInternetArchiveFunction(ExtensionLoader &loader) {
	// Register the internet_archive table function
	// Usage: SELECT * FROM internet_archive() WHERE url = 'archive.org' LIMIT 10
	// Usage with max_results: SELECT * FROM internet_archive(max_results := 500) WHERE url = 'archive.org'
	// - URL filtering via WHERE clause
	// - Supports matchType detection (exact, prefix, host, domain)
	// - Much simpler than common_crawl - no WARC parsing needed
	// - Projection pushdown: only fetches response when needed
	// - Optional max_results parameter controls CDX API result size (default: 100)
	TableFunctionSet internet_archive_set("internet_archive");

	auto ia_func = TableFunction({},
	                              InternetArchiveScan, InternetArchiveBind, InternetArchiveInitGlobal);
	ia_func.cardinality = InternetArchiveCardinality;
	ia_func.pushdown_complex_filter = InternetArchivePushdownComplexFilter;
	ia_func.projection_pushdown = true;

	// Add named parameter
	ia_func.named_parameters["max_results"] = LogicalType::BIGINT;
	ia_func.named_parameters["collapse"] = LogicalType::VARCHAR;

	internet_archive_set.AddFunction(ia_func);

	loader.RegisterFunction(internet_archive_set);
}

} // namespace duckdb
