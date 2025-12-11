#include "common_crawl_utils.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

// ========================================
// GLOBAL VARIABLES
// ========================================

std::chrono::steady_clock::time_point g_start_time;
CollInfoCache g_collinfo_cache;

// ========================================
// TIMING UTILITIES
// ========================================

double ElapsedMs() {
	auto now = std::chrono::steady_clock::now();
	return std::chrono::duration<double, std::milli>(now - g_start_time).count();
}

// ========================================
// STRING UTILITIES
// ========================================

string LikeToRegex(const string &like_pattern) {
	string regex;
	bool starts_with_percent = !like_pattern.empty() && like_pattern[0] == '%';
	bool ends_with_percent = !like_pattern.empty() && like_pattern.back() == '%';

	// Add start anchor if pattern doesn't start with %
	if (!starts_with_percent) {
		regex = "^";
	}

	for (size_t i = 0; i < like_pattern.size(); i++) {
		char c = like_pattern[i];
		if (c == '%') {
			regex += ".*";
		} else if (c == '_') {
			regex += ".";
		} else if (c == '.' || c == '(' || c == ')' || c == '[' || c == ']' ||
		           c == '{' || c == '}' || c == '+' || c == '?' || c == '^' ||
		           c == '$' || c == '|' || c == '\\') {
			// Escape regex special chars
			regex += '\\';
			regex += c;
		} else {
			regex += c;
		}
	}

	// Add end anchor if pattern doesn't end with %
	if (!ends_with_percent) {
		regex += "$";
	}

	return regex;
}

string ToCdxTimestamp(const string &ts_str) {
	string digits;
	for (char c : ts_str) {
		if (c >= '0' && c <= '9') digits += c;
	}
	// Truncate to 14 chars max
	if (digits.length() > 14) {
		digits = digits.substr(0, 14);
	}
	// Strip trailing zeros
	while (digits.length() > 4 && digits.back() == '0') {
		digits.pop_back();
	}
	return digits;
}

string SanitizeUTF8(const string &str) {
	string result;
	result.reserve(str.size());

	for (size_t i = 0; i < str.size(); ) {
		unsigned char c = static_cast<unsigned char>(str[i]);

		// ASCII character (0-127)
		if (c < 0x80) {
			result += c;
			i++;
			continue;
		}

		// Multi-byte UTF-8 character
		int len = 0;
		if ((c & 0xE0) == 0xC0) len = 2;      // 2-byte
		else if ((c & 0xF0) == 0xE0) len = 3; // 3-byte
		else if ((c & 0xF8) == 0xF0) len = 4; // 4-byte
		else {
			// Invalid start byte, replace with ?
			result += '?';
			i++;
			continue;
		}

		// Check if we have enough bytes
		if (i + len > str.size()) {
			// Truncated sequence, replace with ?
			result += '?';
			break;
		}

		// Validate continuation bytes
		bool valid = true;
		for (int j = 1; j < len; j++) {
			if ((static_cast<unsigned char>(str[i + j]) & 0xC0) != 0x80) {
				valid = false;
				break;
			}
		}

		if (valid) {
			// Add the valid multi-byte sequence
			result.append(str, i, len);
			i += len;
		} else {
			// Invalid sequence, replace with ?
			result += '?';
			i++;
		}
	}

	return result;
}

Value SafeStringValue(const string &str) {
	try {
		string sanitized = SanitizeUTF8(str);
		return Value(sanitized);
	} catch (const std::exception &ex) {
		// If sanitization still fails, return empty string
		fprintf(stderr, "[ERROR] Failed to create Value for string: %s (error: %s)\n",
		        str.substr(0, 100).c_str(), ex.what());
		return Value("");
	} catch (...) {
		fprintf(stderr, "[ERROR] Failed to create Value for string: %s (unknown error)\n",
		        str.substr(0, 100).c_str());
		return Value("");
	}
}

string ExtractJSONValue(const string &json_line, const string &key) {
	string search = "\"" + key + "\": \"";
	size_t start = json_line.find(search);
	if (start == string::npos) {
		// Try without space after colon
		search = "\"" + key + "\":\"";
		start = json_line.find(search);
		if (start == string::npos) {
			return "";
		}
	}
	start += search.length();
	size_t end = json_line.find("\"", start);
	if (end == string::npos) {
		return "";
	}
	return SanitizeUTF8(json_line.substr(start, end - start));
}

string ConvertSQLWildcardsToCDX(const string &pattern) {
	string result;
	result.reserve(pattern.length());

	for (char ch : pattern) {
		if (ch == '%') {
			// SQL % (zero or more chars) -> CDX * (zero or more chars)
			result += '*';
		} else if (ch == '_') {
			// SQL _ (single char) -> CDX ? (single char, if supported)
			// Note: CDX API may not support ?, but we'll convert it anyway
			result += '?';
		} else {
			result += ch;
		}
	}

	return result;
}

timestamp_t ParseCDXTimestamp(const string &cdx_timestamp) {
	if (cdx_timestamp.length() != 14) {
		return timestamp_t(0); // Return epoch if invalid format
	}

	try {
		int32_t year = std::stoi(cdx_timestamp.substr(0, 4));
		int32_t month = std::stoi(cdx_timestamp.substr(4, 2));
		int32_t day = std::stoi(cdx_timestamp.substr(6, 2));
		int32_t hour = std::stoi(cdx_timestamp.substr(8, 2));
		int32_t minute = std::stoi(cdx_timestamp.substr(10, 2));
		int32_t second = std::stoi(cdx_timestamp.substr(12, 2));

		// Use DuckDB's Timestamp::FromDatetime to create timestamp
		date_t date = Date::FromDate(year, month, day);
		dtime_t time = Time::FromTime(hour, minute, second, 0);
		return Timestamp::FromDatetime(date, time);
	} catch (...) {
		return timestamp_t(0); // Return epoch on parse error
	}
}

// ========================================
// GZIP DECOMPRESSION
// ========================================

string DecompressGzip(const char *compressed_data, size_t compressed_size) {
	// Initialize zlib stream
	z_stream stream;
	memset(&stream, 0, sizeof(stream));

	// Initialize for gzip decompression (windowBits = 15 + 16 for gzip format)
	int ret = inflateInit2(&stream, 15 + 16);
	if (ret != Z_OK) {
		return "[Error: Failed to initialize gzip decompression]";
	}

	// Set input
	stream.avail_in = compressed_size;
	stream.next_in = (Bytef *)compressed_data;

	// Prepare output buffer (estimate 10x compression ratio)
	std::vector<char> decompressed_buffer;
	decompressed_buffer.reserve(compressed_size * 10);

	// Decompress in chunks
	const size_t chunk_size = 32768; // 32 KB chunks
	char out_buffer[chunk_size];

	do {
		stream.avail_out = chunk_size;
		stream.next_out = (Bytef *)out_buffer;

		ret = inflate(&stream, Z_NO_FLUSH);

		if (ret != Z_OK && ret != Z_STREAM_END) {
			inflateEnd(&stream);
			return "[Error: Gzip decompression failed with code " + to_string(ret) + "]";
		}

		size_t produced = chunk_size - stream.avail_out;
		decompressed_buffer.insert(decompressed_buffer.end(), out_buffer, out_buffer + produced);

	} while (ret != Z_STREAM_END);

	inflateEnd(&stream);

	// Convert to string
	return string(decompressed_buffer.begin(), decompressed_buffer.end());
}

// ========================================
// HTTP/WARC PARSING
// ========================================

unordered_map<string, string> ParseHeaders(const string &header_text) {
	unordered_map<string, string> headers;

	size_t pos = 0;
	size_t line_end;

	while (pos < header_text.length()) {
		// Find end of line
		line_end = header_text.find("\r\n", pos);
		if (line_end == string::npos) {
			line_end = header_text.find("\n", pos);
			if (line_end == string::npos) {
				break;
			}
		}

		string line = header_text.substr(pos, line_end - pos);

		// Find colon separator
		size_t colon_pos = line.find(": ");
		if (colon_pos != string::npos) {
			string key = line.substr(0, colon_pos);
			string value = line.substr(colon_pos + 2);

			// If key already exists, concatenate with ", "
			auto it = headers.find(key);
			if (it != headers.end()) {
				it->second += ", " + value;
			} else {
				headers[key] = value;
			}
		}

		// Move to next line
		pos = line_end + 1;
		if (pos < header_text.length() && header_text[pos] == '\n') {
			pos++;
		}
	}

	return headers;
}

WARCResponse ParseWARCResponse(const string &warc_data) {
	WARCResponse result;

	// WARC format structure:
	// 1. WARC version line + headers (metadata about the record)
	// 2. HTTP status line + headers
	// 3. HTTP body (actual content)

	// Find the end of WARC headers (double newline)
	size_t warc_headers_end = warc_data.find("\r\n\r\n");
	size_t newline_size = 4;
	if (warc_headers_end == string::npos) {
		warc_headers_end = warc_data.find("\n\n");
		newline_size = 2;
		if (warc_headers_end == string::npos) {
			return result; // Invalid WARC format
		}
	}

	// Extract WARC section
	string warc_section = warc_data.substr(0, warc_headers_end);

	// Parse WARC version from first line (e.g., "WARC/1.0")
	size_t first_line_end = warc_section.find("\r\n");
	if (first_line_end == string::npos) {
		first_line_end = warc_section.find("\n");
	}

	if (first_line_end != string::npos) {
		string version_line = warc_section.substr(0, first_line_end);
		if (version_line.find("WARC/") == 0) {
			result.warc_version = version_line.substr(5); // Extract version after "WARC/"
		}

		// Parse remaining WARC headers (skip version line)
		size_t warc_headers_start = first_line_end + 1;
		if (warc_headers_start < warc_section.length() && warc_section[warc_headers_start] == '\n') {
			warc_headers_start++;
		}
		string warc_headers_text = warc_section.substr(warc_headers_start);
		result.warc_headers = ParseHeaders(warc_headers_text);
	}

	// After WARC headers comes the HTTP response
	size_t http_start = warc_headers_end + newline_size;
	size_t http_headers_end = warc_data.find("\r\n\r\n", http_start);
	size_t http_newline_size = 4;
	if (http_headers_end == string::npos) {
		http_headers_end = warc_data.find("\n\n", http_start);
		http_newline_size = 2;
		if (http_headers_end == string::npos) {
			return result; // Invalid HTTP format
		}
	}

	// Extract HTTP section
	string http_section = warc_data.substr(http_start, http_headers_end - http_start);

	// Parse HTTP status line (e.g., "HTTP/1.1 200")
	size_t http_first_line_end = http_section.find("\r\n");
	if (http_first_line_end == string::npos) {
		http_first_line_end = http_section.find("\n");
	}

	if (http_first_line_end != string::npos) {
		string status_line = http_section.substr(0, http_first_line_end);

		// Parse "HTTP/1.1 200 OK" format
		size_t space1 = status_line.find(" ");
		if (space1 != string::npos && status_line.find("HTTP/") == 0) {
			// Extract version (e.g., "1.1" from "HTTP/1.1")
			result.http_version = status_line.substr(5, space1 - 5);

			// Extract status code
			size_t space2 = status_line.find(" ", space1 + 1);
			string status_str;
			if (space2 != string::npos) {
				status_str = status_line.substr(space1 + 1, space2 - space1 - 1);
			} else {
				status_str = status_line.substr(space1 + 1);
			}

			try {
				result.http_status_code = std::stoi(status_str);
			} catch (...) {
				result.http_status_code = 0;
			}
		}

		// Parse HTTP headers (skip status line)
		size_t http_headers_start = http_first_line_end + 1;
		if (http_headers_start < http_section.length() && http_section[http_headers_start] == '\n') {
			http_headers_start++;
		}
		string http_headers_text = http_section.substr(http_headers_start);
		result.http_headers = ParseHeaders(http_headers_text);
	}

	// Extract HTTP body
	result.body = warc_data.substr(http_headers_end + http_newline_size);

	return result;
}

// ========================================
// COLLINFO CACHE
// ========================================

string GetLatestCrawlId(ClientContext &context) {
	// Check if cache is valid and not expired
	if (!g_collinfo_cache.IsExpired()) {
		return g_collinfo_cache.latest_crawl_id;
	}

	// Fetch collinfo.json using httpfs and json extension
	fprintf(stderr, "[DEBUG] Fetching latest crawl_id from collinfo.json\n");

	string collinfo_url = "https://index.commoncrawl.org/collinfo.json";
	Connection con(context.db->GetDatabase(context));

	// Ensure json extension is loaded
	con.Query("INSTALL json");
	con.Query("LOAD json");

	// Read the first entry from collinfo.json (newest crawl)
	auto result = con.Query("SELECT id FROM read_json('" + collinfo_url + "') LIMIT 1");

	if (result->HasError()) {
		throw IOException("Failed to fetch collinfo.json: " + result->GetError());
	}

	if (result->RowCount() == 0) {
		throw IOException("collinfo.json returned no results");
	}

	auto latest_id = result->GetValue(0, 0).ToString();

	// Update cache
	g_collinfo_cache.latest_crawl_id = latest_id;
	g_collinfo_cache.cached_at = std::chrono::system_clock::now();
	g_collinfo_cache.is_valid = true;

	fprintf(stderr, "[DEBUG] Latest crawl_id: %s\n", latest_id.c_str());
	return latest_id;
}

} // namespace duckdb
