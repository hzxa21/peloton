//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// op_expression.h
//
// Identification: src/include/optimizer/op_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/group.h"
#include "optimizer/operator_node.h"

#include <memory>
#include <vector>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Operator Expr
//===--------------------------------------------------------------------===//
class OperatorExpression {
 public:
  OperatorExpression(Operator op);

  void PushChild(std::shared_ptr<OperatorExpression> op);

  void PopChild();

  const std::vector<std::shared_ptr<OperatorExpression>> &Children() const;

  const Operator &Op() const;

  void SetDistinct(std::vector<std::shared_ptr<expression::AbstractExpression>> distinct_column_exprs) {
    distinct_column_exprs_ = distinct_column_exprs;
  }

  inline std::vector<std::shared_ptr<expression::AbstractExpression>> GetDistinct() {
    return distinct_column_exprs_;
  }

 private:
  Operator op;
  std::vector<std::shared_ptr<OperatorExpression>> children;
  std::vector<std::shared_ptr<expression::AbstractExpression>> distinct_column_exprs_;
};

} // namespace optimizer
} // namespace peloton
