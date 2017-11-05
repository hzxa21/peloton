//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_rewrite_rule_impls.h
//
// Identification: src/executor/plan_rewrite_rule_impls.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/plan_rewrite_rule_impls.h"
#include "expression/tuple_value_expression.h"
#include "planner/abstract_scan_plan.h"
#include "planner/hash_join_plan.h"
#include "planner/prefilter_plan.h"

namespace peloton {
namespace executor {

void RobustExecution::Rewrite(planner::AbstractPlan *root) const {
  // Insert a PrefilterPlan under all consecutive right deep hash joins
  std::vector<planner::HashJoinPlan *> hash_join_plans;
  TraverseAndInsert(root, hash_join_plans);
}

void RobustExecution::TraverseAndInsert(
    planner::AbstractPlan *plan,
    std::vector<planner::HashJoinPlan *> &hash_join_plans) const {
  if (plan->GetPlanNodeType() == PlanNodeType::HASHJOIN) {
    hash_join_plans.push_back(static_cast<planner::HashJoinPlan *>(plan));
    if (plan->GetChild(1)->GetPlanNodeType() != PlanNodeType::HASHJOIN) {
      // Consecutive right deep hash ends
      if (hash_join_plans.size() >= 2) {
        // We found more than 2 consecutive right deep hash joins
        // TODO: Change this. Assume the right child is a scan
        std::vector<const planner::AttributeInfo *> ais;
        ((planner::AbstractScan *)plan->GetChild(1))->GetAttributes(ais);
        std::unordered_set<const planner::AttributeInfo *> ais_set(ais.begin(),
                                                                   ais.end());
        // Check if all the right join keys are from the right child
        bool all_from_right = true;
        for (auto *hash_join : hash_join_plans) {
          std::vector<const expression::AbstractExpression *> right_keys;
          hash_join->GetRightHashKeys(right_keys);
          for (auto *right_key : right_keys) {
            // Only support Tuple Value expression key for now
            auto *ai = ((expression::TupleValueExpression *)right_key)
                           ->GetAttributeRef();
            if (ais_set.find(ai) == ais_set.end()) {
              all_from_right = false;
              break;
            }
          }
          if (!all_from_right) {
            break;
          }
        }

        if (all_from_right) {
          // Insert a PrefilterPlan
          auto *bottom_hash_join = hash_join_plans.back();
          auto orig_right_child = bottom_hash_join->RemoveChild();
          std::unique_ptr<planner::AbstractPlan> prefilterPlan(
              new planner::PrefilterPlan(hash_join_plans));
          prefilterPlan->AddChild(move(orig_right_child));
          bottom_hash_join->AddChild(move(prefilterPlan));
        }
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

}  // namespace executor
}  // namespace peloton
