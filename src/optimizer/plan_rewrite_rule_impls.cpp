//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_rewrite_rule_impls.h
//
// Identification: src/optimizer/plan_rewrite_rule_impls.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/plan_rewrite_rule_impls.h"
#include "planner/prefilter_plan.h"

namespace peloton {
namespace optimizer {

void RobustExecution::Rewrite(planner::AbstractPlan *root) const {
  // Insert a PrefilterPlan under all consecutive right deep hash joins
  std::vector<planner::AbstractPlan *> hash_join_plans;
  TraverseAndInsert(root, hash_join_plans);
}

void RobustExecution::TraverseAndInsert(
    planner::AbstractPlan *plan,
    std::vector<planner::AbstractPlan *> &hash_join_plans) const {
  if (plan->GetPlanNodeType() == PlanNodeType::HASHJOIN) {
    hash_join_plans.push_back(plan);
    if (plan->GetChild(1)->GetPlanNodeType() != PlanNodeType::HASHJOIN) {
      // Consecutive right deep hash ends
      if (hash_join_plans.size() >= 2) {
        // We found more than 2 consecutive right deep hash joins
        // Insert a PrefilterPlan
        auto *bottom_hash_join = hash_join_plans.back();
        auto orig_right_child = bottom_hash_join->RemoveChild();
        std::unique_ptr<planner::AbstractPlan> prefilterPlan(
            new planner::PrefilterPLan(hash_join_plans));
        prefilterPlan->AddChild(move(orig_right_child));
        bottom_hash_join->AddChild(move(prefilterPlan));
      }
      hash_join_plans.clear();
    }
  }
  // Traverse to the right child first
  if (plan->GetChildrenSize() == 2) {
    TraverseAndInsert(plan->GetModifiableChild(1), hash_join_plans);
    assert(hash_join_plans.size() == 0);
    TraverseAndInsert(plan->GetModifiableChild(0), hash_join_plans);
  } else if (plan->GetChildrenSize() == 1) {
    assert(hash_join_plans.size() == 0);
    TraverseAndInsert(plan->GetModifiableChild(0), hash_join_plans);
  }
}
}
}