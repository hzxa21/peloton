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
#include "codegen/proxy/timer_set_proxy.h"
#include "planner/hash_join_plan.h"
#include "planner/prefilter_plan.h"

//#define USE_TIMER

namespace peloton {
namespace codegen {

// Constructor
PrefilterTranslator::PrefilterTranslator(
    const planner::PrefilterPlan &prefilter, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), prefilter_(prefilter) {
  // Prepare translator for the left child
  context.Prepare(*prefilter.GetChild(0), pipeline);

#ifdef USE_TIMER
  timer_set_id_ = context.GetRuntimeState().RegisterState(
      "timer_set", TimerSetProxy::GetType(GetCodeGen()));
#endif
}

void PrefilterTranslator::InitializeState() {
  // Extract the state id for all the bloom filter in the right deep join tree
  for (planner::HashJoinPlan *hash_join_plan : prefilter_.GetHashJoinPlans()) {
    auto *translator = GetCompilationContext().GetTranslator(
        *((planner::AbstractPlan *)hash_join_plan));
    bloom_filter_ids_.push_back(
        static_cast<HashJoinTranslator *>(translator)->bloom_filter_id_);
  }
#ifdef USE_TIMER
  GetCodeGen().Call(TimerSetProxy::Init, {LoadStatePtr(timer_set_id_)});
#endif
}

void PrefilterTranslator::TearDownState() {
#ifdef USE_TIMER
  // Print the timer
  llvm::Value *hash_probe_time =
      GetCodeGen()->CreateFSub(GetDuration(3), GetDuration(2));
  llvm::Value *bloom_probe_time =
      GetCodeGen()->CreateFSub(GetDuration(1), GetDuration(0));
  bloom_probe_time = GetCodeGen()->CreateFSub(
      bloom_probe_time,
      GetCodeGen()->CreateFMul(GetCodeGen().ConstDouble(3), GetDuration(2)));
  bloom_probe_time = GetCodeGen()->CreateFSub(bloom_probe_time, GetDuration(3));
  GetCodeGen().CallPrintf(
      "Hash Probe Time: %d ms, Bloom Filter Probe Time: %d ms\n",
      {hash_probe_time, bloom_probe_time});

  // Destroy the timer
  GetCodeGen().Call(TimerSetProxy::Destroy, {LoadStatePtr(timer_set_id_)});
#endif
}

void PrefilterTranslator::Produce() const {
  GetCompilationContext().Produce(*prefilter_.GetChild(0));
}

void PrefilterTranslator::Consume(ConsumerContext &context,
                                  RowBatch::Row &row) const {
  std::vector<std::unique_ptr<lang::If>> nested_ifs;

#ifdef USE_TIMER
  StartTimer(0);
  StopTimer(0);
  StartTimer(1);
#endif

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

#ifdef USE_TIMER
  StartTimer(2);
  StopTimer(2);
  StartTimer(3);
#endif

  // If a tuple passes all the bloom fiters, start probing the hash tables
  context.Consume(row);

#ifdef USE_TIMER
  StopTimer(3);
#endif

  // End all the nested ifs
  while (!nested_ifs.empty()) {
    nested_ifs.back()->EndIf();
    nested_ifs.pop_back();
  }
#ifdef USE_TIMER
  StopTimer(1);
#endif
}

void PrefilterTranslator::StartTimer(uint32_t timer_id) const {
  llvm::Value *timer_set = LoadStatePtr(timer_set_id_);
  GetCodeGen().Call(TimerSetProxy::Start,
                    {timer_set, GetCodeGen().Const32(timer_id)});
}

void PrefilterTranslator::StopTimer(uint32_t timer_id) const {
  llvm::Value *timer_set = LoadStatePtr(timer_set_id_);
  GetCodeGen().Call(TimerSetProxy::Stop,
                    {timer_set, GetCodeGen().Const32(timer_id)});
}

llvm::Value *PrefilterTranslator::GetDuration(uint32_t timer_id) const {
  llvm::Value *timer_set = LoadStatePtr(timer_set_id_);
  return GetCodeGen().Call(TimerSetProxy::GetDuration,
                           {timer_set, GetCodeGen().Const32(timer_id)});
}

std::string PrefilterTranslator::GetName() const { return "Prefilter"; }

}  // namespace codegen
}  // namespace peloton
