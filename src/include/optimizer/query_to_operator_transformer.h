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
      bool convertible, std::shared_ptr<OperatorExpression> expr,
      std::unordered_set<std::string> table_alias_set,
      expression::AbstractExpression *inner_key = nullptr)
      : is_convertible(convertible),
        output_expr(expr),
        table_alias_set(table_alias_set),
        subquery_query_key_expr(inner_key) {}

  bool is_convertible;
  std::shared_ptr<OperatorExpression> output_expr;
  std::unordered_set<std::string> table_alias_set;
  // Store two key expression separately rather than the join condition
  // expression
  // because there can be restrictions on the inner key (e.g. unique, limit 1)
  expression::AbstractExpression *subquery_query_key_expr;
};

typedef std::vector<std::shared_ptr<SubqueryOperatorExpressionContext>>
    SubqueryContexts;

// Transform a query from parsed statement to operator expressions.
class QueryToOperatorTransformer : public SqlNodeVisitor {
 public:
  QueryToOperatorTransformer(concurrency::Transaction *txn);

  std::shared_ptr<OperatorExpression> ConvertToOpExpression(
      parser::SQLStatement *op);

  void Visit( parser::SelectStatement *op) override;

  void Visit( parser::TableRef *) override;
  void Visit( parser::JoinDefinition *) override;
  void Visit( parser::GroupByDescription *) override;
  void Visit( parser::OrderDescription *) override;
  void Visit( parser::LimitDescription *) override;

  void Visit( parser::CreateStatement *op) override;
  void Visit( parser::InsertStatement *op) override;
  void Visit( parser::DeleteStatement *op) override;
  void Visit( parser::DropStatement *op) override;
  void Visit( parser::PrepareStatement *op) override;
  void Visit( parser::ExecuteStatement *op) override;
  void Visit( parser::TransactionStatement *op) override;
  void Visit( parser::UpdateStatement *op) override;
  void Visit( parser::CopyStatement *op) override;
  void Visit( parser::AnalyzeStatement *op) override;
  void Visit(expression::SubqueryExpression *expr) override;

  inline oid_t GetAndIncreaseGetId() { return get_id++; }

  // Helper functions
  bool ConvertSubquery(expression::AbstractExpression *expr);

  std::shared_ptr<expression::AbstractExpression> GenerateHavingPredicate(
      expression::AbstractExpression *having_expr = nullptr);

  std::shared_ptr<expression::AbstractExpression> GenerateHavingPredicate(
      const parser::SelectStatement *op);

 private:
  std::shared_ptr<OperatorExpression> output_expr_;
  MultiTablePredicates join_predicates_;
  SingleTablePredicatesMap single_table_predicates_map;
  std::unordered_set<std::string> table_alias_set_;
  std::unordered_map<int, std::vector<expression::AbstractExpression *>>
      predicates_by_depth_;
  SubqueryContexts subquery_contexts_;
  bool is_subquery_convertible_;
  int depth_;

  concurrency::Transaction *txn_;
  // identifier for get operators
  oid_t get_id;
  bool enable_predicate_push_down_;
};

}  // namespace optimizer
}  // namespace peloton
