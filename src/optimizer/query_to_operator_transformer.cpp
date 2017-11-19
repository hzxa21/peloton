//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_to_operator_transformer.cpp
//
// Identification: src/optimizer/query_to_operator_transformer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cmath>
#include <include/settings/settings_manager.h>

#include "expression/expression_util.h"
#include "expression/subquery_expression.h"

#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "optimizer/query_node_visitor.h"
#include "optimizer/query_to_operator_transformer.h"

#include "planner/seq_scan_plan.h"

#include "parser/statements.h"

#include "catalog/manager.h"

using std::vector;
using std::shared_ptr;

namespace peloton {
namespace optimizer {
QueryToOperatorTransformer::QueryToOperatorTransformer(
    concurrency::Transaction *txn)
    : txn_(txn),
      get_id(0),
      enable_predicate_push_down_(settings::SettingsManager::GetBool(
          settings::SettingId::predicate_push_down)) {}
std::shared_ptr<OperatorExpression>
QueryToOperatorTransformer::ConvertToOpExpression(parser::SQLStatement *op) {
  output_expr_ = nullptr;
  op->Accept(this);
  return output_expr_;
}

void QueryToOperatorTransformer::Visit(parser::SelectStatement *op) {
  // Store previous subquery contexts;
  auto pre_subquery_contexts = std::move(subquery_contexts_);
  subquery_contexts_ = SubqueryContexts();

  // Set depth of the current query
  depth_ = op->depth;

  LOG_DEBUG("Visit select query with depth [%d]", depth_);

  // Init the convertible flag
  // Only if the current query has aggregation
  // and contains non-equality predicate correlated to the outer query
  bool current_query_convertible = true;

  // Checkpoint the index for the outer query
  std::unordered_map<int, size_t> outer_predicate_idx;
  for (auto &entry : predicates_by_depth_) {
    if (entry.first < depth_) continue;
    outer_predicate_idx[entry.first] = entry.second.size();
  }

  // Whether the query require aggregation, if it does, generate having
  // predicates
  // first because the predicates from outer query should be placed after the
  // aggregation
  // e.g. SELECT * FROM (select avg(b) as bb from test group by A) as A WHERE bb
  // < 33
  bool require_agg = util::RequireAggregation(op);
  std::shared_ptr<expression::AbstractExpression> having_pred = nullptr;
  vector<shared_ptr<expression::AbstractExpression>> group_by_cols;
  if (require_agg) {
    having_pred = GenerateHavingPredicate(op);
    if (op->group_by != nullptr)
      for (auto &col : op->group_by->columns)
        group_by_cols.emplace_back(col->Copy());
  }

  if (op->where_clause != nullptr) {
    // Split by AND
    std::vector<expression::AbstractExpression *> predicates;
    util::SplitPredicates(op->where_clause.get(), predicates);
    for (auto pred : predicates) {
      if (!pred->HasSubquery() || !ConvertSubquery(pred))
        // If the predicate does not have subquery or
        // the subquery cannot be converted to join, we need to keep the
        // predicate
        predicates_by_depth_[pred->GetDepth()].push_back(pred);
    }
    // Extract single table predicates and join predicates from the where clause
    auto &cur_predicates = predicates_by_depth_[depth_];
    util::ExtractPredicates(cur_predicates, single_table_predicates_map,
                            join_predicates_, enable_predicate_push_down_);
    PL_ASSERT(single_table_predicates_map.size() + join_predicates_.size() ==
        cur_predicates.size());

    LOG_DEBUG("Predicate size = %ld", cur_predicates.size());


    // Check whether the query can be converted into a join with the outer
    // query.
    // The query is convertible if one of the following is true:
    // 1. The query does not contain aggregation
    // 2. The query contains aggregation and all the predicates correlated with
    //    the outer query is in the form of IN.col = OUT.col
    if (require_agg) {
      vector<shared_ptr<expression::AbstractExpression>> new_group_by_cols;
      for (auto &entry : predicates_by_depth_) {
        auto outer_depth = entry.first;
        if (outer_depth >= depth_) continue;
        if (!current_query_convertible) break;
        auto outer_predicates = predicates_by_depth_[outer_depth];
        auto outer_idx = outer_predicate_idx.count(outer_depth) == 0
                             ? 0
                             : outer_predicate_idx[outer_depth];
        // Iterate through all the correlated predicates generated within
        // current query
        for (auto i = outer_idx; i < outer_predicates.size(); i++) {
          auto pred = outer_predicates[i];
          if (pred->GetChildrenSize() != 2 ||
              (pred->GetChild(0)->GetDepth() < depth_ &&
               pred->GetChild(1)->GetDepth() < depth_))
            continue;
          auto type = pred->GetExpressionType();
          if (type == ExpressionType::COMPARE_EQUAL) {
            auto left_expr = pred->GetChild(0);
            auto right_expr = pred->GetChild(1);
            auto cur_depth_expr =
                left_expr->GetDepth() < depth_ ? right_expr : left_expr;
            new_group_by_cols.emplace_back(cur_depth_expr->Copy());
          } else {
            current_query_convertible = false;
            break;
          }
        }
      }
      if (current_query_convertible)
        group_by_cols.insert(group_by_cols.end(), new_group_by_cols.begin(),
                             new_group_by_cols.end());
    }

    // Clear predicate info of the current query
    cur_predicates.clear();
  }

  if (op->from_table != nullptr) {
    // SELECT with FROM

    // Clear table alias set
    table_alias_set_.clear();

    op->from_table->Accept(this);

    // Handle subquery operator trees
    for (auto &context : subquery_contexts_) {
      util::SetUnion(table_alias_set_, context->table_alias_set);
      if (!context->is_convertible) {
        // TODO
        throw Exception("Subquery not convertible");
      } else {
        auto child_expr = output_expr_;
        auto predicates =
            util::ConstructJoinPredicate(table_alias_set_, join_predicates_);
        output_expr_ = std::make_shared<OperatorExpression>(
            LogicalInnerJoin::make(predicates));
        output_expr_->PushChild(child_expr);
        output_expr_->PushChild(context->output_expr);
      }
    }
    PL_ASSERT(join_predicates_.empty());

    // Add aggregation on top of the current tree if any
    if (require_agg) {
      if (!group_by_cols.empty()) {
        auto group_by_expr = std::make_shared<OperatorExpression>(
            LogicalGroupBy::make(move(group_by_cols), having_pred));
        group_by_expr->PushChild(output_expr_);
        output_expr_ = group_by_expr;
      } else {
        auto agg_expr = std::make_shared<OperatorExpression>(
            LogicalAggregate::make(having_pred));
        agg_expr->PushChild(output_expr_);
        output_expr_ = agg_expr;
      }
    }

  } else {
    // SELECT without FROM
    output_expr_ = std::make_shared<OperatorExpression>(LogicalGet::make());
  }

  // Set convertible flag
  is_subquery_convertible_ = current_query_convertible;

  // Clear predicate info
  single_table_predicates_map.clear();
  join_predicates_.clear();

  // Restore subquery contexts
  subquery_contexts_ = std::move(pre_subquery_contexts);

  LOG_DEBUG("Finish visiting select query with depth [%d]", depth_);
}
void QueryToOperatorTransformer::Visit(parser::JoinDefinition *node) {
  // Get left operator
  node->left->Accept(this);
  auto left_expr = output_expr_;
  auto left_table_alias_set = table_alias_set_;
  // If not do this, when traversing the right subtree, table_alias_set will
  // not be empty, which is incorrect.
  table_alias_set_.clear();

  // Get right operator
  node->right->Accept(this);
  auto right_expr = output_expr_;
  util::SetUnion(table_alias_set_, left_table_alias_set);

  // Construct join operator
  std::shared_ptr<OperatorExpression> join_expr;
  switch (node->type) {
    case JoinType::INNER: {
      if (node->condition != nullptr) {
        // Add join condition into join predicates
        std::unordered_set<std::string> join_condition_table_alias_set;
        expression::ExpressionUtil::GenerateTableAliasSet(
            node->condition.get(), join_condition_table_alias_set);
        join_predicates_.emplace_back(
            AnnotatedExpression(std::shared_ptr<expression::AbstractExpression>(
                                    node->condition->Copy()),
                                join_condition_table_alias_set));
      }
      // Based on the set of all table alias in the subtree, extract those
      // join predicates that applies to this join.
      join_expr = std::make_shared<OperatorExpression>(LogicalInnerJoin::make(
          util::ConstructJoinPredicate(table_alias_set_, join_predicates_)));
      break;
    }
    case JoinType::OUTER: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalOuterJoin::make(node->condition->Copy()));
      break;
    }
    case JoinType::LEFT: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalLeftJoin::make(node->condition->Copy()));
      break;
    }
    case JoinType::RIGHT: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalRightJoin::make(node->condition->Copy()));
      break;
    }
    case JoinType::SEMI: {
      join_expr = std::make_shared<OperatorExpression>(
          LogicalSemiJoin::make(node->condition->Copy()));
      break;
    }
    default:
      throw Exception("Join type invalid");
  }

  join_expr->PushChild(left_expr);
  join_expr->PushChild(right_expr);

  output_expr_ = join_expr;

}

void QueryToOperatorTransformer::Visit(parser::TableRef *node) {
  // Nested select. Not supported in the current executors
  if (node->select != nullptr) {
    // Store previous context
    auto pre_join_predicates = join_predicates_;
    auto pre_single_table_predicates_map = single_table_predicates_map;
    auto pre_table_alias_set = table_alias_set_;
    join_predicates_.clear();
    single_table_predicates_map.clear();
    table_alias_set_.clear();

    // Construct query derived table predicates
    auto table_alias = StringUtil::Lower(node->GetTableAlias());
    auto alias_to_expr_map =
        util::ConstructSelectElementMap(node->select->select_list);
    auto predicates = pre_single_table_predicates_map[table_alias];
    for (auto &original_predicate : predicates) {
      util::ExtractPredicates(util::TransformQueryDerivedTablePredicates(
                                  alias_to_expr_map, original_predicate.get()),
                              single_table_predicates_map, join_predicates_,
                              enable_predicate_push_down_);
    }

    node->select->Accept(this);

    auto alias = StringUtil::Lower(node->GetTableAlias());
    pre_table_alias_set.insert(alias);
    join_predicates_ = pre_join_predicates;
    single_table_predicates_map = pre_single_table_predicates_map;
    table_alias_set_ = pre_table_alias_set;

    auto child_expr = output_expr_;
    output_expr_ =
        std::make_shared<OperatorExpression>(LogicalQueryDerivedGet::make(
            GetAndIncreaseGetId(), alias, alias_to_expr_map));
    output_expr_->PushChild(child_expr);

  }
  // Explicit Join
  else if (node->join != nullptr) {
    node->join->Accept(this);
  }
  // Multiple tables (Implicit Join)
  else if (node->list.size() > 1) {
    // Create a join operator between the first two tables
    node->list.at(0)->Accept(this);
    auto left_expr = output_expr_;
    auto left_table_alias_set = table_alias_set_;
    table_alias_set_.clear();

    node->list.at(1)->Accept(this);
    auto right_expr = output_expr_;
    util::SetUnion(table_alias_set_, left_table_alias_set);

    auto join_expr =
        std::make_shared<OperatorExpression>(LogicalInnerJoin::make(
            util::ConstructJoinPredicate(table_alias_set_, join_predicates_)));
    join_expr->PushChild(left_expr);
    join_expr->PushChild(right_expr);

    // Build a left deep join tree
    for (size_t i = 2; i < node->list.size(); i++) {
      node->list.at(i)->Accept(this);
      auto old_join_expr = join_expr;
      join_expr = std::make_shared<OperatorExpression>(LogicalInnerJoin::make(
          util::ConstructJoinPredicate(table_alias_set_, join_predicates_)));
      join_expr->PushChild(old_join_expr);
      join_expr->PushChild(output_expr_);
    }
    output_expr_ = join_expr;
  }
  // Single table
  else {
    if (node->list.size() == 1) node = node->list.at(0).get();
    storage::DataTable *target_table =
        catalog::Catalog::GetInstance()->GetTableWithName(
            node->GetDatabaseName(), node->GetTableName(), txn_);
    std::string table_alias =
        StringUtil::Lower(std::string(node->GetTableAlias()));
    // Update table alias map
    table_alias_set_.insert(table_alias);
    // Construct logical operator
    auto predicates_entry = single_table_predicates_map.find(table_alias);
    if (predicates_entry != single_table_predicates_map.end())
      output_expr_ = std::make_shared<OperatorExpression>(LogicalGet::make(
          GetAndIncreaseGetId(), target_table, node->GetTableAlias(),
          std::shared_ptr<expression::AbstractExpression>(
              util::CombinePredicates(predicates_entry->second))));
    else
      output_expr_ = std::make_shared<OperatorExpression>(LogicalGet::make(
          GetAndIncreaseGetId(), target_table, node->GetTableAlias()));
  }
}

void QueryToOperatorTransformer::Visit( parser::GroupByDescription *) {}
void QueryToOperatorTransformer::Visit( parser::OrderDescription *) {}
void QueryToOperatorTransformer::Visit( parser::LimitDescription *) {}

void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE  parser::CreateStatement *op) {}
void QueryToOperatorTransformer::Visit( parser::InsertStatement *op) {
  storage::DataTable *target_table =
      catalog::Catalog::GetInstance()->GetTableWithName(
          op->GetDatabaseName(), op->GetTableName(), txn_);
  if (op->type == InsertType::SELECT) {
    auto insert_expr = std::make_shared<OperatorExpression>(
        LogicalInsertSelect::make(target_table));
    op->select->Accept(this);
    insert_expr->PushChild(output_expr_);
    output_expr_ = insert_expr;
  } else {
    auto insert_expr = std::make_shared<OperatorExpression>(
        LogicalInsert::make(target_table, &op->columns, &op->insert_values));
    output_expr_ = insert_expr;
  }
}

void QueryToOperatorTransformer::Visit( parser::DeleteStatement *op) {
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      op->GetDatabaseName(), op->GetTableName(), txn_);
  std::shared_ptr<OperatorExpression> table_scan;
  if (op->expr != nullptr)
    table_scan = std::make_shared<OperatorExpression>(LogicalGet::make(
        GetAndIncreaseGetId(), target_table, op->GetTableName(),
        std::shared_ptr<expression::AbstractExpression>(op->expr->Copy())));
  else
    table_scan = std::make_shared<OperatorExpression>(LogicalGet::make(
        GetAndIncreaseGetId(), target_table, op->GetTableName()));
  auto delete_expr =
      std::make_shared<OperatorExpression>(LogicalDelete::make(target_table));
  delete_expr->PushChild(table_scan);

  output_expr_ = delete_expr;
}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE  parser::DropStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE  parser::PrepareStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE  parser::ExecuteStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE  parser::TransactionStatement *op) {}
void QueryToOperatorTransformer::Visit( parser::UpdateStatement *op) {
  auto target_table = catalog::Catalog::GetInstance()->GetTableWithName(
      op->table->GetDatabaseName(), op->table->GetTableName(), txn_);
  std::shared_ptr<OperatorExpression> table_scan;

  auto update_expr = std::make_shared<OperatorExpression>(
      LogicalUpdate::make(target_table, &op->updates));

  if (op->where != nullptr)
    table_scan = std::make_shared<OperatorExpression>(LogicalGet::make(
        GetAndIncreaseGetId(), target_table, op->table->GetTableName(),
        std::shared_ptr<expression::AbstractExpression>(op->where->Copy()),
        true));
  else
    table_scan = std::make_shared<OperatorExpression>(
        LogicalGet::make(GetAndIncreaseGetId(), target_table,
                         op->table->GetTableName(), nullptr, true));

  update_expr->PushChild(table_scan);

  output_expr_ = update_expr;
}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE  parser::CopyStatement *op) {}
void QueryToOperatorTransformer::Visit(
    UNUSED_ATTRIBUTE  parser::AnalyzeStatement *op) {}

void QueryToOperatorTransformer::Visit(expression::SubqueryExpression *expr) {
  auto cur_depth = depth_;
  //  auto expr_depth = expr->GetDepth();
  //  // Independent inner query
  //  if (expr_depth > cur_depth) {
  //    throw Exception("Not support independent subquery");
  //  } else { // Correlated subquery
  expr->GetSubSelect()->Accept(this);
  //  }
  depth_ = cur_depth;
}

//===----------------------------------------------------------------------===//
// Aggregation helper functions
//===----------------------------------------------------------------------===//
std::shared_ptr<expression::AbstractExpression>
QueryToOperatorTransformer::GenerateHavingPredicate(
    const parser::SelectStatement *op) {
  if (op->group_by != nullptr) {
    return GenerateHavingPredicate(op->group_by->having.get());
  } else {
    return GenerateHavingPredicate();
  }
}

std::shared_ptr<expression::AbstractExpression>
QueryToOperatorTransformer::GenerateHavingPredicate(
    expression::AbstractExpression *having_expr) {
  auto having_predicates =
      std::vector<shared_ptr<expression::AbstractExpression>>();
  if (having_expr != nullptr) {
    having_predicates.push_back(
        std::shared_ptr<expression::AbstractExpression>(having_expr->Copy()));
  }
  // If there are any predicates from the upper level, they should be added into
  // the
  // having clause
  for (auto &entry : single_table_predicates_map)
    for (auto &pred : entry.second) having_predicates.emplace_back(pred);
  for (auto &expr : join_predicates_) having_predicates.emplace_back(expr.expr);

  std::shared_ptr<expression::AbstractExpression> having = nullptr;
  if (having_predicates.size() == 1)
    having = having_predicates[0];
  else
    std::shared_ptr<expression::AbstractExpression>(
        util::CombinePredicates(having_predicates));

  // Clear the predicates
  single_table_predicates_map.clear();
  join_predicates_.clear();

  return having;
}

//===----------------------------------------------------------------------===//
// Subquery helper functions
//===----------------------------------------------------------------------===//

// Convert a expr with subquery to a normal join if one of the following is
// true:
// 1. It is a OPERATOR_EXISTS predicate and the subquery
//    is correlated with the outer query with {<,<=,>,>=,=,!=} relationship
// 2. It is a COMPARE_IN predicate
// 3. It is a {<,<=,>,>=,=,!=} predicate with only one child of it is a subquery
// Note: if the expr contains disjunction, it cannot be converted to join unless
// we have implemented a mark join.
bool QueryToOperatorTransformer::ConvertSubquery(
    expression::AbstractExpression *expr) {
  auto expr_type = expr->GetExpressionType();
  if (expr_type == ExpressionType::OPERATOR_EXISTS) {
    auto pre_predicate_size = predicates_by_depth_[depth_].size();
    auto subquery_expr = dynamic_cast<expression::SubqueryExpression *>(
        expr->GetModifiableChild(0));
    PL_ASSERT(subquery_expr != nullptr);
    auto sub_select = subquery_expr->GetSubSelect();
    sub_select->select_list.clear();
    sub_select->group_by.release();

    // Get subquery operator expression tree
    subquery_expr->Accept(this);

    if (is_subquery_convertible_) {
      auto updated_predicate_size = predicates_by_depth_[depth_].size();
      // If there are new predicates, that means the subquery
      // has predicates correlated to the current query
      if (updated_predicate_size > pre_predicate_size) {
        auto &predicates = predicates_by_depth_[depth_];
        for (auto i = pre_predicate_size; i < updated_predicate_size; i++) {
          auto pred = predicates[i];
          auto pred_type = pred->GetExpressionType();
          switch (pred_type) {
            case ExpressionType::COMPARE_EQUAL:
            case ExpressionType::COMPARE_GREATERTHAN:
            case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            case ExpressionType::COMPARE_LESSTHAN:
            case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
              auto left_expr = pred->GetModifiableChild(0);
              auto right_expr = pred->GetModifiableChild(1);
              auto l_depth = left_expr->GetDepth();
              auto r_depth = right_expr->GetDepth();
              if (l_depth == r_depth)  // Not a valid join predicate
                break;
              auto key_expr = l_depth < r_depth ? right_expr : left_expr;
              subquery_contexts_.push_back(
                  std::make_shared<SubqueryOperatorExpressionContext>(
                      true, output_expr_, table_alias_set_, key_expr));
              return true;
            }
            default:
              ;
          }
        }
      }
    }
    // Cannot find correlated predicates, transforms it into a nested iteration
    // plan
    subquery_contexts_.push_back(
        std::make_shared<SubqueryOperatorExpressionContext>(false, output_expr_,
                                                            table_alias_set_));
    return false;
  } else if (expr_type == ExpressionType::COMPARE_IN) {
    auto subquery_expr = dynamic_cast<expression::SubqueryExpression *>(
        expr->GetModifiableChild(1));
    PL_ASSERT(subquery_expr != nullptr);
    // Check select element in the subquery
    auto sub_select = subquery_expr->GetSubSelect();
    auto select_ele = sub_select->select_list.at(0).get();
    if (sub_select->select_list.size() != 1 ||
        select_ele->GetExpressionType() == ExpressionType::STAR)
      throw Exception("Not valid select element in subquery");

    // Get subquery operator expression tree
    subquery_expr->Accept(this);

    if (is_subquery_convertible_) {
      auto left_expr = expr->GetModifiableChild(0);
      expression::AbstractExpression *join_predicate =
          new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL,
                                               left_expr->Copy(),
                                               select_ele->Copy());
      auto pred_depth = join_predicate->DeriveDepth();
      predicates_by_depth_[pred_depth].push_back(join_predicate);
      subquery_contexts_.push_back(
          std::make_shared<SubqueryOperatorExpressionContext>(
              true, output_expr_, table_alias_set_, select_ele));

      return true;
    }
  } else if (expr_type == ExpressionType::COMPARE_EQUAL ||
             expr_type == ExpressionType::COMPARE_GREATERTHAN ||
             expr_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
             expr_type == ExpressionType::COMPARE_LESSTHAN ||
             expr_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
    auto left_expr = expr->GetModifiableChild(0);
    auto right_expr = expr->GetModifiableChild(1);
    auto is_left_sub =
        left_expr->GetExpressionType() == ExpressionType::ROW_SUBQUERY;
    auto is_right_sub =
        right_expr->GetExpressionType() == ExpressionType::ROW_SUBQUERY;

    // The expr is convertible if only one side has the subquery
    if (is_left_sub != is_right_sub) {
      auto join_cond_type = expr_type;
      if (is_left_sub) {
        std::swap(left_expr, right_expr);
        join_cond_type =
            expression::ExpressionUtil::ReverseComparisonExpressionType(
                join_cond_type);
      }
      auto subquery_expr =
          dynamic_cast<expression::SubqueryExpression *>(right_expr);
      PL_ASSERT(subquery_expr != nullptr);
      // Check select element in the subquery
      auto sub_select = subquery_expr->GetSubSelect();
      auto select_ele = sub_select->select_list.at(0).get();
      if (sub_select->select_list.size() != 1 ||
          select_ele->GetExpressionType() == ExpressionType::STAR)
        throw Exception("Not valid select element in subquery");

      // Get subquery operator expression tree
      subquery_expr->Accept(this);

      if (is_subquery_convertible_) {
        expression::AbstractExpression *join_predicate =
            new expression::ComparisonExpression(
                join_cond_type, left_expr->Copy(), select_ele->Copy());
        auto pred_depth = join_predicate->DeriveDepth();
        predicates_by_depth_[pred_depth].push_back(join_predicate);
        subquery_contexts_.push_back(
            std::make_shared<SubqueryOperatorExpressionContext>(
                true, output_expr_, table_alias_set_, select_ele));
        return true;
      }
    }
  }

  // The expr is not convertible, transforms it into a nested iteration plan
  expr->Accept(this);
  subquery_contexts_.push_back(
      std::make_shared<SubqueryOperatorExpressionContext>(false, output_expr_,
                                                          table_alias_set_));
  return false;
}

}  // namespace optimizer
}  // namespace peloton
