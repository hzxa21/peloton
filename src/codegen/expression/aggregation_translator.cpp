//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_value_translator.cpp
//
// Identification: src/codegen/expression/aggregation_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/expression/aggregation_translator.h"

#include "expression/aggregate_expression.h"

namespace peloton {
namespace codegen {

// Constructor
AggregationTranslator::AggregationTranslator(
    const expression::AggregateExpression &agg_expr,
    CompilationContext &context)
    : ExpressionTranslator(agg_expr, context) {
  PL_ASSERT(agg_expr.GetAttributeRef() != nullptr);
}

// Produce the value that is the result of codegening the expression
codegen::Value AggregationTranslator::DeriveValue(CodeGen &codegen,
                                                  RowBatch::Row &row) const {
  const auto &agg_expr = GetExpressionAs<expression::AggregateExpression>();
  return row.DeriveValue(codegen, agg_expr.GetAttributeRef());
}

}  // namespace codegen
}  // namespace peloton