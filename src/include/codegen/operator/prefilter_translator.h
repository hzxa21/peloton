//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// prefilter_translator.h
//
// Identification: src/include/codegen/operator/prefilter_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/bloom_filter_accessor.h"
#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/operator/operator_translator.h"

namespace peloton {

namespace planner {
class PrefilterPlan;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
// The translator for a hash-join operator
//===----------------------------------------------------------------------===//
class PrefilterTranslator : public OperatorTranslator {
 public:
  PrefilterTranslator(const planner::PrefilterPlan &prefilter,
                      CompilationContext &context, Pipeline &pipeline);

  // Codegen any initialization work for this operator
  void InitializeState() override;

  // Codegen any cleanup work for this translator
  void TearDownState() override;

  // Define any helper functions this translator needs
  void DefineAuxiliaryFunctions() override {}

  // The method that produces new tuples
  void Produce() const override;

  // The method that consumes tuples from child operators
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  std::string GetName() const override;

 private:
  void StartTimer(uint32_t timer_id) const;

  void StopTimer(uint32_t timer_id) const;

  llvm::Value *GetDuration(uint32_t timer_id) const;

 private:
  // Prefilter Plan
  const planner::PrefilterPlan &prefilter_;

  // Bloom Filter Codegen Accessor
  BloomFilterAccessor bloom_filter_;

  // Runtime State Id of timer set
  RuntimeState::StateID timer_set_id_;

  // Runtime State ids of all bloom filters in the consecutive right deep tree
  std::vector<RuntimeState::StateID> bloom_filter_ids_;
};

}  // namespace codegen
}  // namespace peloton