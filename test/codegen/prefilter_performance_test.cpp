//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// prefilter_performance_test.cpp
//
// Identification: test/codegen/prefilter_performance_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdlib>
#include <unordered_set>
#include <vector>

#include "codegen/bloom_filter_accessor.h"
#include "codegen/codegen.h"
#include "codegen/counting_consumer.h"
#include "codegen/function_builder.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/bloom_filter_proxy.h"
#include "codegen/testing_codegen_util.h"
#include "codegen/util/bloom_filter.h"
#include "common/timer.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/plan_executor.h"
#include "optimizer/optimizer.h"
#include "planner/hash_join_plan.h"
#include "planner/seq_scan_plan.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class PrefilterCodegenTest : public PelotonTest {
 public:
  PrefilterCodegenTest() {
    // Create test db
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }

  ~PrefilterCodegenTest() {
    // Drop test db
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }

  int UpDivide(int num1, int num2) { return (num1 + num2 - 1) / num2; }

  void InsertTuple(const std::vector<int> &vals, storage::DataTable *table,
                   concurrency::Transaction *txn);

  void CreateTable(std::string table_name, int tuple_size,
                   concurrency::Transaction *txn);

  void LoadTable(std::string table_name, int tuple_size, int target_size,
                 std::vector<int> &numbers, std::unordered_set<int> &number_set,
                 concurrency::Transaction *txn);

  double ExecuteJoin(std::string query, concurrency::Transaction *txn,
                     int num_iter, std::vector<std::vector<int>> &inner_numbers,
                     std::vector<int> &indexes, bool enable_robust_execution);

  const int bigint_size = 8;
  std::vector<std::string> inner_table_names;
  std::string outer_table_name;
};

TEST_F(PrefilterCodegenTest, PerformanceTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *catalog = catalog::Catalog::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  // Parameters
  const unsigned num_of_inners = 3;
  const int inner_tuple_size = 512;
  const int outer_tuple_size = num_of_inners * bigint_size;
  const int L3_cache_size = 15360000;
  const int inner_target_size = L3_cache_size * 30;
  const int outer_to_inner_ratio = 100;
  std::vector<double> selectivities = {0.8, 0.5, 0.2};
  int num_iter = 1;

  // Create inner tables
  for (unsigned i = 0; i < num_of_inners; i++) {
    inner_table_names.push_back("inner_r" + std::to_string(i));
  }
  const std::vector<int> inner_tuple_sizes(num_of_inners, inner_tuple_size);
  for (unsigned i = 0; i < inner_tuple_sizes.size(); i++) {
    CreateTable(inner_table_names[i], inner_tuple_sizes[i], txn);
  }

  // Create outer table
  outer_table_name = "outer_r";
  CreateTable(outer_table_name, outer_tuple_size, txn);

  // Load inner relations
  std::vector<std::vector<int>> inner_numbers(num_of_inners);
  std::vector<std::unordered_set<int>> inner_number_sets(num_of_inners);
  std::vector<int> target_sizes(num_of_inners, inner_target_size);
  for (unsigned i = 0; i < num_of_inners; i++) {
    LoadTable(inner_table_names[i], inner_tuple_sizes[i], target_sizes[i],
              inner_numbers[i], inner_number_sets[i], txn);
  }

  LOG_INFO("Finish populating inner tables");

  // Load outer relation
  auto *outer_table =
      catalog->GetTableWithName(DEFAULT_DB_NAME, outer_table_name, txn);
  PL_ASSERT(selectivities.size() == num_of_inners);
  unsigned outer_table_cardinality =
      inner_numbers[0].size() * outer_to_inner_ratio;
  for (unsigned i = 0; i < outer_table_cardinality; i++) {
    std::vector<int> vals;
    // Construct each column of a single tuple.
    for (unsigned j = 0; j < num_of_inners; j++) {
      int number;
      auto &numbers = inner_numbers[j];
      auto &number_set = inner_number_sets[j];
      if (rand() % 100 < selectivities[j] * 100) {
        // Pick a random number from the inner table
        number = numbers[rand() % numbers.size()];
      } else {
        // Pick a random number that is not in inner table
        do {
          number = rand();
        } while (number_set.count(number) == 1);
      }
      vals.push_back(number);
    }
    // Insert tuple into outer relation
    InsertTuple(vals, outer_table, txn);
  }

  LOG_INFO("Finish populating outer table");

  // Generate the query for each possible order
  std::vector<int> indexes(num_of_inners);
  std::iota(indexes.begin(), indexes.end(), 0);
  do {
    LOG_INFO("\n");
    std::string query = "SELECT * FROM " + outer_table_name;
    for (int index : indexes) {
      query += ", ";
      query += inner_table_names[index];
    }
    query += " WHERE ";
    for (unsigned i = 0; i < num_of_inners; i++) {
      if (i != 0) {
        query += " AND ";
      }
      query += outer_table_name + ".c" + std::to_string(i) + " = ";
      query += inner_table_names[i] + ".c0";
    }

    LOG_INFO("Query: %s", query.c_str());

    // Execute plan without robust execution
    LOG_INFO("Executing without robust execution");
    ExecuteJoin(query, txn, num_iter, inner_numbers, indexes, false);

    // Execute plan with robust execution
    LOG_INFO("Executing with robust execution");
    ExecuteJoin(query, txn, num_iter, inner_numbers, indexes, true);
  } while (std::next_permutation(indexes.begin(), indexes.end()));

  // Commit transaction
  txn_manager.CommitTransaction(txn);
}

double PrefilterCodegenTest::ExecuteJoin(
    std::string query, concurrency::Transaction *txn, int num_iter,
    std::vector<std::vector<int>> &inner_numbers, std::vector<int> &indexes,
    bool enable_robust_execution) {
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());
  double total_runtime = 0;
  // Run hash join multiple times and calculate the average runtime
  for (int i = 0; i < num_iter; i++) {
    auto plan =
        TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);

    // Check the plan structure
    planner::AbstractPlan *plan_ptr = plan.get();
    for (int i = indexes.size() - 1; i >= 0; i--) {
      // Make sure the order is correct
      assert(((planner::SeqScanPlan *)plan_ptr->GetChild(0))
                 ->GetTable()
                 ->GetName() == inner_table_names[indexes[i]]);

      // Change the bloom filter flag and set the correct cardinality in the
      // plan
      const_cast<planner::AbstractPlan *>(plan_ptr->GetChild(0))
          ->SetCardinality(inner_numbers[indexes[i]].size());
      dynamic_cast<planner::HashJoinPlan *>(plan_ptr)->SetBloomFilterFlag(
          enable_robust_execution);
      dynamic_cast<planner::HashJoinPlan *>(plan_ptr)->SetUseBloomFilter(false);

      plan_ptr = const_cast<planner::AbstractPlan *>(plan_ptr->GetChild(1));
    }
    assert(((planner::SeqScanPlan *)plan_ptr)->GetTable()->GetName() ==
           outer_table_name);

    // Binding
    planner::BindingContext context;
    plan->PerformBinding(context);

    // Add our prefilter plan node
    if (enable_robust_execution) {
      executor::PlanExecutor::RewritePlanTree(plan.get());
    }

    // Use simple CountConsumer since we don't care about the result
    codegen::CountingConsumer consumer;
    // Compile the query
    codegen::QueryCompiler compiler;
    auto compiled_query = compiler.Compile(*plan, consumer);
    // Run and collect runtime stats
    codegen::Query::RuntimeStats stats;
    std::unique_ptr<executor::ExecutorContext> executor_context(
        new executor::ExecutorContext{txn});
    compiled_query->Execute(*txn, executor_context.get(),
                            consumer.GetCountAsState(), &stats);

    LOG_INFO("Execution Time: %0.0f ms", stats.plan_ms);
    total_runtime += stats.plan_ms;
  }
  return total_runtime / num_iter;
}

void PrefilterCodegenTest::LoadTable(std::string table_name, int tuple_size,
                                     int target_size, std::vector<int> &numbers,
                                     std::unordered_set<int> &number_set,
                                     concurrency::Transaction *txn) {
  int curr_size = 0;
  auto *catalog = catalog::Catalog::GetInstance();
  auto *table = catalog->GetTableWithName(DEFAULT_DB_NAME, table_name, txn);
  while (curr_size < target_size) {
    // Find a unique random number
    int random;
    do {
      random = rand();
    } while (number_set.count(random) == 1);
    numbers.push_back(random);
    number_set.insert(random);

    // Insert tuple into the table
    std::vector<int> vals(UpDivide(tuple_size, bigint_size), random);
    InsertTuple(vals, table, txn);

    curr_size += tuple_size;
  }
}

// Create a table where all the columns are BIGINT and each tuple has desired
// tuple size
void PrefilterCodegenTest::CreateTable(std::string table_name, int tuple_size,
                                       concurrency::Transaction *txn) {
  int curr_size = 0;
  size_t bigint_size = type::Type::GetTypeSize(type::TypeId::BIGINT);
  std::vector<catalog::Column> cols;
  while (curr_size < tuple_size) {
    cols.push_back(
        catalog::Column{type::TypeId::BIGINT, bigint_size,
                        "c" + std::to_string(curr_size / bigint_size), true});
    curr_size += bigint_size;
  }
  auto *catalog = catalog::Catalog::GetInstance();
  std::unique_ptr<catalog::Schema> schema(new catalog::Schema(cols));
  catalog->CreateTable(DEFAULT_DB_NAME, table_name, std::move(schema), txn);
}

// Insert a tuple to specific table
void PrefilterCodegenTest::InsertTuple(const std::vector<int> &vals,
                                       storage::DataTable *table,
                                       concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::Tuple tuple{table->GetSchema(), true};
  for (unsigned i = 0; i < vals.size(); i++) {
    tuple.SetValue(i, type::ValueFactory::GetBigIntValue(vals[i]));
  }
  ItemPointer *index_entry_ptr = nullptr;
  auto tuple_slot_id = table->InsertTuple(&tuple, txn, &index_entry_ptr);
  PL_ASSERT(tuple_slot_id.block != INVALID_OID);
  PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
  txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
}

}  // namespace test
}  // namespace peloton
