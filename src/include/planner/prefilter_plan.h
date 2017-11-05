//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// limit_plan.h
//
// Identification: src/include/planner/prefilter_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_plan.h"

namespace peloton {
namespace planner {

class HashJoinPlan;

/**
 * @brief This plan is inserted into plan tree if the robust execution is
 * enabled and optimizer finds a right deep join tree subplan. It's used to
 * prefilter the tuples before probing any hash tables.
 */
class PrefilterPlan : public AbstractPlan {
 public:
  PrefilterPlan(std::vector<HashJoinPlan *> &hash_joins)
      : hash_joins_(hash_joins) {}

  inline PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::PREFILTER;
  }

  const std::vector<HashJoinPlan *> &GetHashJoinPlans() const {
    return hash_joins_;
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    // Copy does not make sense for this type of plan
    std::vector<HashJoinPlan *> hash_joins;
    return std::unique_ptr<AbstractPlan>(new PrefilterPlan(hash_joins));
  }

 private:
  // Hash Join Plans that we should do prefeilter on.
  std::vector<HashJoinPlan *> hash_joins_;
};

}  // namespace planner
}  // namespace peloton