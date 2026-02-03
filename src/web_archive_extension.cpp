#define DUCKDB_EXTENSION_MAIN

#include "web_archive_extension.hpp"
#include "web_archive_utils.hpp"
#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

// Feature detection: DuckDB 1.5+ has ExtensionCallbackManager, 1.4.x doesn't
#if __has_include("duckdb/main/extension_callback_manager.hpp")
#define DUCKDB_HAS_NEW_OPTIMIZER_API 1
#else
#define DUCKDB_HAS_NEW_OPTIMIZER_API 0
#endif

namespace duckdb {

// Forward declarations from common_crawl_index.cpp and wayback_machine (internet_archive.cpp)
void OptimizeCommonCrawlLimitPushdown(unique_ptr<LogicalOperator> &op);
void OptimizeWaybackMachineLimitPushdown(unique_ptr<LogicalOperator> &op);
void OptimizeWaybackMachineDistinctOnPushdown(unique_ptr<LogicalOperator> &op);

// Combined optimizer for both table functions
static void WebArchiveOptimizerFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeCommonCrawlLimitPushdown(plan);
	OptimizeWaybackMachineLimitPushdown(plan);
	OptimizeWaybackMachineDistinctOnPushdown(plan);
}

#if DUCKDB_HAS_NEW_OPTIMIZER_API
// DuckDB 1.5+ uses class-based OptimizerExtension::Register API
class WebArchiveOptimizerExtension : public OptimizerExtension {
public:
	WebArchiveOptimizerExtension() {
		optimize_function = WebArchiveOptimizerFunction;
	}
};
#endif

static void LoadInternal(ExtensionLoader &loader) {
	// Note: httpfs extension must be loaded before using this extension
	// Users should run: INSTALL httpfs; LOAD httpfs; before loading this extension
	// Or set autoload_known_extensions=1 and autoinstall_known_extensions=1

	// Register the common_crawl_index table function
	RegisterCommonCrawlFunction(loader);

	// Register the wayback_machine table function
	RegisterWaybackMachineFunction(loader);

	// Register optimizer extension for LIMIT pushdown
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
#if DUCKDB_HAS_NEW_OPTIMIZER_API
	// DuckDB 1.5+ API
	OptimizerExtension::Register(config, WebArchiveOptimizerExtension());
#else
	// DuckDB 1.4.x API
	OptimizerExtension optimizer;
	optimizer.optimize_function = WebArchiveOptimizerFunction;
	config.optimizer_extensions.push_back(std::move(optimizer));
#endif
}

void WebArchiveExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string WebArchiveExtension::Name() {
	return "web_archive";
}

std::string WebArchiveExtension::Version() const {
#ifdef EXT_VERSION_WEB_ARCHIVE_CDX
	return EXT_VERSION_WEB_ARCHIVE_CDX;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(web_archive, loader) {
	duckdb::LoadInternal(loader);
}
}
