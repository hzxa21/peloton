//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// prefilter_translator.cpp
//
// Identification: src/codegen/operator/prefilter_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/prefilter_translator.h"
#include "codegen/operator/hash_join_translator.h"
#include "planner/hash_join_plan.h"
#include "planner/prefilter_plan.h"

namespace peloton {
namespace codegen {

// Constructor
PrefilterTranslator::PrefilterTranslator(
    const planner::PrefilterPlan &prefilter, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), prefilter_(prefilter) {
  // Prepare translator for the left child
  context.Prepare(*prefilter.GetChild(0), pipeline);
}

void PrefilterTranslator::InitializeState() {
  // Extract the state id for all the bloom filter in the right deep join tree
  for (planner::HashJoinPlan *hash_join_plan : prefilter_.GetHashJoinPlans()) {
    auto *translator = GetCompilationContext().GetTranslator(
        *((planner::AbstractPlan *)hash_join_plan));
    bloom_filter_ids_.push_back(
        static_cast<HashJoinTranslator *>(translator)->bloom_filter_id_);
  }
}

void PrefilterTranslator::Produce() const {
  GetCompilationContext().Produce(*prefilter_.GetChild(0));
}

void PrefilterTranslator::Consume(ConsumerContext &context,
                                  RowBatch::Row &row) const {
  std::vector<std::unique_ptr<lang::If>> nested_ifs;

  for (int i = bloom_filter_ids_.size() - 1; i >= 0; i--) {
    // Get right join keys
    std::vector<const expression::AbstractExpression *> right_keys;
    prefilter_.GetHashJoinPlans()[i]->GetRightHashKeys(right_keys);

    // Get codegen values for right join key
    std::vector<codegen::Value> key;
    for (const auto *exp : right_keys) {
      key.push_back(row.DeriveValue(GetCodeGen(), *exp));
    }

    // Probe the ith bloom filter.
    llvm::Value *contains = bloom_filter_.Contains(
        GetCodeGen(), LoadStatePtr(bloom_filter_ids_[i]), key);

    nested_ifs.emplace_back(new lang::If{GetCodeGen(), contains});
  }

  // If a tuple passes all the bloom fiters, start probing the hash tables
  context.Consume(row);

  // End all the nested ifs
  while (!nested_ifs.empty()) {
    nested_ifs.back()->EndIf();
    nested_ifs.pop_back();
  }
}

std::string PrefilterTranslator::GetName() const { return "Prefilter"; }

}  // namespace codegen
}  // namespace peloton
