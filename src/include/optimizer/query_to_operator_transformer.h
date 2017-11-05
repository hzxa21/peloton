//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_to_operator_transformer.h
//
// Identification: src/include/optimizer/query_to_operator_transformer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>
#include "common/sql_node_visitor.h"

namespace peloton {

namespace parser {
class SQLStatement;
}  // namespace parser

namespace concurrency {
class Transaction;
}

namespace expression {
class AbstractExpression;
}

namespace optimizer {

class OperatorExpression;

class SubqueryOperatorExpressionContext {
 public:
  SubqueryOperatorExpressionContext(
      bool convertible,
      std::shared_ptr<OperatorExpression> expr,
      std::unordered_set<std::string> table_alias_set,
      expression::AbstractExpression* outer_key = nullptr,
      expression::AbstractExpression* inner_key = nullptr,
      ExpressionType type = ExpressionType::COMPARE_EQUAL)
      : is_convertible(convertible), output_expr(expr),
        table_alias_set(table_alias_set),
        outer_query_key_expr(outer_key),
        inner_query_key_expr(inner_key), join_condition_type(type) {}

  bool is_convertible;
  std::shared_ptr<OperatorExpression> output_expr;
  std::unordered_set<std::string> table_alias_set;
  // Store two key expression separately rather than the join condition expression
  // because there can be restrictions on the inner key (e.g. unique, limit 1)
  expression::AbstractExpression* outer_query_key_expr;
  expression::AbstractExpression* inner_query_key_expr;
  ExpressionType join_condition_type;
};

typedef std::vector<std::shared_ptr<SubqueryOperatorExpressionContext>> SubqueryContexts;

// Transform a query from parsed statement to operator expressions.
class QueryToOperatorTransformer : public SqlNodeVisitor {
 public:
  QueryToOperatorTransformer(concurrency::Transaction *txn);

  std::shared_ptr<OperatorExpression> ConvertToOpExpression(
      parser::SQLStatement *op);

  void Visit(const parser::SelectStatement *op) override;

  void Visit(const parser::TableRef *) override;
  void Visit(const parser::JoinDefinition *) override;
  void Visit(const parser::GroupByDescription *) override;
  void Visit(const parser::OrderDescription *) override;
  void Visit(const parser::LimitDescription *) override;

  void Visit(const parser::CreateStatement *op) override;
  void Visit(const parser::InsertStatement *op) override;
  void Visit(const parser::DeleteStatement *op) override;
  void Visit(const parser::DropStatement *op) override;
  void Visit(const parser::PrepareStatement *op) override;
  void Visit(const parser::ExecuteStatement *op) override;
  void Visit(const parser::TransactionStatement *op) override;
  void Visit(const parser::UpdateStatement *op) override;
  void Visit(const parser::CopyStatement *op) override;
  void Visit(const parser::AnalyzeStatement *op) override;
  void Visit(expression::SubqueryExpression *expr) override;

  inline oid_t GetAndIncreaseGetId() { return get_id++; }

  bool ConvertSubquery(expression::AbstractExpression* expr);
  void MaybeRewriteSubqueryWithAggregation(parser::SelectStatement *select);

 private:
  std::shared_ptr<OperatorExpression> output_expr_;
  MultiTablePredicates join_predicates_;
  SingleTablePredicatesMap single_table_predicates_map;
  std::unordered_set<std::string> table_alias_set_;
  std::unordered_map<int, std::vector<expression::AbstractExpression*>> predicates_by_depth_;
  std::unordered_map<int, SubqueryContexts> subquery_by_depth_;
  int depth_;

  concurrency::Transaction *txn_;
  // identifier for get operators
  oid_t get_id;
  bool enable_predicate_push_down_;


};

}  // namespace optimizer
}  // namespace peloton
