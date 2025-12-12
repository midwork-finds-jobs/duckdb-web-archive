#define DUCKDB_EXTENSION_MAIN

#include "common_crawl_extension.hpp"
#include "common_crawl_utils.hpp"
#include "duckdb.hpp"
#include "duckdb/main/config.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

// Forward declarations from common_crawl_index.cpp and internet_archive.cpp
void OptimizeCommonCrawlLimitPushdown(unique_ptr<LogicalOperator> &op);
void OptimizeInternetArchiveLimitPushdown(unique_ptr<LogicalOperator> &op);

// Combined optimizer for both table functions
void CommonCrawlOptimizer(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeCommonCrawlLimitPushdown(plan);
	OptimizeInternetArchiveLimitPushdown(plan);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Note: httpfs extension must be loaded before using this extension
	// Users should run: INSTALL httpfs; LOAD httpfs; before loading this extension
	// Or set autoload_known_extensions=1 and autoinstall_known_extensions=1

	// Register the common_crawl_index table function
	RegisterCommonCrawlFunction(loader);

	// Register the internet_archive table function
	RegisterInternetArchiveFunction(loader);

	// Register optimizer extension for LIMIT pushdown
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	OptimizerExtension optimizer;
	optimizer.optimize_function = CommonCrawlOptimizer;
	config.optimizer_extensions.push_back(std::move(optimizer));
}

void CommonCrawlExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string CommonCrawlExtension::Name() {
	return "common_crawl";
}

std::string CommonCrawlExtension::Version() const {
#ifdef EXT_VERSION_COMMON_CRAWL
	return EXT_VERSION_COMMON_CRAWL;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(common_crawl, loader) {
	duckdb::LoadInternal(loader);
}
}
