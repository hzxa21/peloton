//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_rewrite_rule.h
//
// Identification: src/include/executor/plan_rewrite_rule.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

namespace peloton {
namespace executor {

class PlanRewriteRule {
 public:
  virtual void Rewrite(planner::AbstractPlan *root) const = 0;
};

}  // namespace executor
}  // namespace peloton
