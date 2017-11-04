//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_rewrite_rule_impls.h
//
// Identification: src/include/optimizer/plan_rewrite_rule_impls.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/plan_rewrite_rule.h"

namespace peloton {
namespace optimizer {

// Robust Execution Optimization. When a right deep join
// subplan is found, we can build a bloom filter from each inner relation and
// insert a prefilter_plan node at the bottom of the tree to do a prefilter
// on tuples using those bloom filters before probing any hash tables.
class RobustExecution : public PlanRewriteRule {
 public:
  void Rewrite(planner::AbstractPlan *root) const override;

 private:
  void TraverseAndInsert(
      planner::AbstractPlan *plan,
      std::vector<planner::AbstractPlan *> &hash_join_plans) const;
};

}  // namespace optimizer
}  // namespace peloton