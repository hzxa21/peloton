//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_value_expression.h
//
// Identification: src/include/expression/subquery_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"

#include "type/types.h"
#include "common/logger.h"
#include "common/sql_node_visitor.h"
#include "planner/binding_context.h"
#include "util/string_util.h"

namespace peloton {
namespace expression {

class SubqueryExpression : AbstractExpression {

 protected:
  std::unique_ptr<parser::SelectStatement> select_;



};

}  // namespace expression
}  // namespace peloton