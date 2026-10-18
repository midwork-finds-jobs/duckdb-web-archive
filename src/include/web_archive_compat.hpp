#pragma once

// Source compatibility between DuckDB 1.4/1.5 and DuckDB 2.0 (main).
//
// DuckDB 2.0 made expression members private (children, function, value), turned comparisons and
// BETWEEN into BoundFunctionExpressions (the old classes are now static helper sets), made
// identifiers a distinct `Identifier` type, made FlatVector::GetData<T> read-only (writes use
// GetDataMutable<T>) and split the vector helpers into their own headers. The helpers below give
// the filter-pushdown and scan code one spelling that compiles against both.

#include "duckdb.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#if __has_include("duckdb/common/vector/flat_vector.hpp")
#define WEB_ARCHIVE_DUCKDB_V2 1
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#endif

namespace duckdb {

//! Writable flat-vector data.
template <class T>
inline T *WaData(Vector &vector) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return FlatVector::GetDataMutable<T>(vector);
#else
	return FlatVector::GetData<T>(vector);
#endif
}

//! Child expressions of a function / operator / conjunction expression.
#ifdef WEB_ARCHIVE_DUCKDB_V2
inline vector<unique_ptr<Expression>> &WaChildren(BoundFunctionExpression &expr) {
	return expr.GetChildrenMutable();
}
inline vector<unique_ptr<Expression>> &WaChildren(BoundOperatorExpression &expr) {
	return expr.GetChildrenMutable();
}
inline vector<unique_ptr<Expression>> &WaChildren(BoundConjunctionExpression &expr) {
	return expr.GetChildrenMutable();
}
inline const vector<unique_ptr<Expression>> &WaChildren(const BoundFunctionExpression &expr) {
	return expr.GetChildren();
}
inline const vector<unique_ptr<Expression>> &WaChildren(const BoundOperatorExpression &expr) {
	return expr.GetChildren();
}
inline const vector<unique_ptr<Expression>> &WaChildren(const BoundConjunctionExpression &expr) {
	return expr.GetChildren();
}
#else
template <class EXPR>
inline auto &WaChildren(EXPR &expr) {
	return expr.children;
}
#endif

//! Value of a constant expression.
inline const Value &WaValue(const BoundConstantExpression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return expr.GetValue();
#else
	return expr.value;
#endif
}

//! Name of a bound scalar function (e.g. "prefix", "suffix", "contains").
inline string WaFunctionName(const BoundFunctionExpression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return string(expr.Function().GetName());
#else
	return expr.function.name;
#endif
}

//! Name of the table function a LogicalGet scans (e.g. "wayback_machine").
inline string WaTableFunctionName(const LogicalGet &get) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return string(get.function.GetName());
#else
	return get.function.name;
#endif
}

//! Column binding of a bound column reference.
inline const ColumnBinding &WaBinding(const BoundColumnRefExpression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return expr.Binding();
#else
	return expr.binding;
#endif
}

//! Index of a bound reference expression.
inline idx_t WaIndex(const BoundReferenceExpression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return expr.Index();
#else
	return expr.index;
#endif
}

//! CAST expressions (a BoundFunctionExpression in DuckDB 2.0).
inline bool WaIsCast(const Expression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return BoundCastExpression::IsCast(expr);
#else
	return expr.GetExpressionClass() == ExpressionClass::BOUND_CAST;
#endif
}

inline const Expression &WaCastChild(const Expression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return BoundCastExpression::Child(expr.Cast<BoundFunctionExpression>());
#else
	return *expr.Cast<BoundCastExpression>().child;
#endif
}

//! A STRUCT child vector (StructVector::GetEntries holds Vectors in 2.0, unique_ptrs before).
inline Vector *WaVec(Vector &vector) {
	return &vector;
}
inline Vector *WaVec(unique_ptr<Vector> &vector) {
	return vector.get();
}

//! Declare an optional named (keyword) parameter of a table function. DuckDB 2.0 treats a keyword
//! parameter without a default as required, so it gets a NULL default the bind callback skips.
inline void WaAddNamedParameter(TableFunction &function, const string &name, const LogicalType &type) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	function.GetSignature().AddKeywordOnly(Identifier(name), type, Value(type));
#else
	function.named_parameters[name] = type;
#endif
}

//! Column-name list a table function's bind callback fills in.
#ifdef WEB_ARCHIVE_DUCKDB_V2
using WaNames = vector<Identifier>;
#else
using WaNames = vector<string>;
#endif

inline vector<string> WaStrings(const WaNames &names) {
	vector<string> result;
	result.reserve(names.size());
	for (auto &name : names) {
		result.push_back(string(name));
	}
	return result;
}

//! Name of a bound column reference as a plain string.
inline string WaColumnName(const BoundColumnRefExpression &expr) {
	return string(expr.GetName());
}

//! Alias of an expression as a plain string.
inline string WaAlias(const Expression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return string(expr.GetAlias());
#else
	return expr.alias;
#endif
}

//! A comparison expression's operands (`left <op> right`).
struct WaComparison {
	unique_ptr<Expression> &left;
	unique_ptr<Expression> &right;
};

inline bool WaIsComparison(const Expression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	       BoundComparisonExpression::IsComparison(expr);
#else
	return expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON;
#endif
}

inline WaComparison WaAsComparison(Expression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	auto &func = expr.Cast<BoundFunctionExpression>();
	return {BoundComparisonExpression::LeftMutable(func), BoundComparisonExpression::RightMutable(func)};
#else
	auto &comparison = expr.Cast<BoundComparisonExpression>();
	return {comparison.left, comparison.right};
#endif
}

//! A BETWEEN expression's operands (`input BETWEEN lower AND upper`).
struct WaBetween {
	unique_ptr<Expression> &input;
	unique_ptr<Expression> &lower;
	unique_ptr<Expression> &upper;
	bool lower_inclusive;
	bool upper_inclusive;
};

inline bool WaIsBetween(const Expression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	return expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	       expr.GetExpressionType() == ExpressionType::COMPARE_BETWEEN;
#else
	return expr.GetExpressionClass() == ExpressionClass::BOUND_BETWEEN;
#endif
}

inline WaBetween WaAsBetween(Expression &expr) {
#ifdef WEB_ARCHIVE_DUCKDB_V2
	auto &func = expr.Cast<BoundFunctionExpression>();
	return {BoundBetweenExpression::InputMutable(func), BoundBetweenExpression::LowerBoundMutable(func),
	        BoundBetweenExpression::UpperBoundMutable(func), BoundBetweenExpression::LowerInclusive(func),
	        BoundBetweenExpression::UpperInclusive(func)};
#else
	auto &between = expr.Cast<BoundBetweenExpression>();
	return {between.input, between.lower, between.upper, between.lower_inclusive, between.upper_inclusive};
#endif
}

} // namespace duckdb
