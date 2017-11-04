
#include <cstdio>
#include <getopt.h>
#include <string>
#include <include/planner/plan_util.h>
#include "catalog/catalog.h"
#include "common/logger.h"
#include "common/statement.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "executor/insert_executor.h"
#include "executor/plan_executor.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "sql/testing_sql_util.h"
#include "planner/seq_scan_plan.h"
#include "planner/abstract_join_plan.h"
#include "planner/hash_join_plan.h"
#include "traffic_cop/traffic_cop.h"
#include "expression/tuple_value_expression.h"
#include "settings/settings_manager.h"
#include "executor/plan_executor.cpp"

namespace peloton {
namespace performance {

double selectivity = 0.1;
size_t table1_table_size = 600000;
int iter = 3;
bool reversed = false;
int num_join = 1;

void Usage(FILE *out) {
  fprintf(out,
          "Command line options : tpch <options> \n"
              "   -h              :  print help message \n"
              "   -s              :  selectivity \n"
              "   -i              :  iteration \n"
              "   -r              :  reverse the order of execution \n"
              "   -n              :  number of joins \n"
              "   -t              :  table size \n");
}

void ParseArguments(int argc, char **argv) {
  // Parse args
  while (1) {
    int c = getopt(argc, argv, "hrs:t:n:");

    if (c == -1) break;

    switch (c) {
      case 's': {
        char *input = optarg;
        selectivity = std::atof(input);
        break;
      }
      case 't': {
        char *input = optarg;
        table1_table_size = static_cast<uint32_t>(std::atoi(input));
        break;
      }
      case 'i': {
        char *input = optarg;
        iter = (std::atoi(input));
        break;
      }
      case 'n': {
        char *input = optarg;
        num_join = (std::atoi(input));
        break;
      }
      case 'r': {
        reversed = true;
        break;
      }
      case 'h': {
        Usage(stderr);
        exit(EXIT_FAILURE);
      }
      default: {
        LOG_ERROR("Unknown option: -%c-", c);
        Usage(stderr);
        exit(EXIT_FAILURE);
      }
    }
  }
}

void PrintConfig() {
  printf("selectivity=%f\ntable_size=%ld\niter=%d\nreversed=%d\n", selectivity, table1_table_size, iter, reversed);
}

class OptimizerTests {
 public:
  OptimizerTests() : txn_manager_(concurrency::TransactionManagerFactory::GetInstance()) {
    // Create test db
    LOG_INFO("Create default db...");
    auto txn = txn_manager_.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager_.CommitTransaction(txn);
  }

  ~OptimizerTests() {
    // Drop test db
    LOG_INFO("Destroy default db...");
    auto txn = txn_manager_.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager_.CommitTransaction(txn);
  }

  void CreateTable(std::string table_name, int tuple_size, concurrency::Transaction *txn, bool set_primary = true) {
    int curr_size = 0;
    size_t bigint_size = type::Type::GetTypeSize(type::TypeId::BIGINT);
    std::vector<catalog::Column> cols;
    bool first = true;
    while (curr_size < tuple_size) {
      auto col = catalog::Column{type::TypeId::BIGINT, bigint_size,
                                 "c" + std::to_string(curr_size / bigint_size), true};
      col.AddConstraint(catalog::Constraint(ConstraintType::NOTNULL, "con_not_null"));
      if (first && set_primary) {
        col.AddConstraint(catalog::Constraint(ConstraintType::PRIMARY, "con_primary"));
        first = false;
      }
      cols.push_back(col);
      curr_size += bigint_size;
    }
    auto *catalog = catalog::Catalog::GetInstance();
    catalog->CreateTable(DEFAULT_DB_NAME, table_name,
                         std::make_unique<catalog::Schema>(cols), txn);
  }

  void InsertTuple(const std::vector<int> &vals, storage::DataTable *table,
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

  std::vector<int> CreateAndLoadTable(size_t type_size,
                                      size_t tuple_size,
                                      size_t table_size,
                                      double join_selectivity,
                                      std::vector<int>& candidate_res) {
    auto *txn = txn_manager_.BeginTransaction();
    auto table_name1 = "test1";
    auto table_name2 = "test2";
    CreateTable(table_name1, tuple_size, txn, true);
    CreateTable(table_name2, tuple_size, txn, false);
    std::vector<int> res;
    std::unordered_set<int> res_set;
    size_t curr_size = 0;
    size_t val_size = (tuple_size + type_size - 1) / type_size;
    auto *table1 = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, table_name1, txn);
    auto *table2 = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, table_name2, txn);
    while (curr_size < table_size) {
      // Find a unique random number
      int random;
      if (rand() % 100 < join_selectivity * 100) {
        random = candidate_res[rand() * candidate_res.size()];
      }
      else {
        do {
          random = rand();
        } while (res_set.count(random) == 1);
        res.push_back(random);
        res_set.insert(random);
      }

      // Insert tuple into the table1
      std::vector<int> vals(val_size, random);
      InsertTuple(vals, table1, txn);
      InsertTuple(vals, table2, txn);
      InsertTuple(vals, table2, txn);

      curr_size += tuple_size;
    }

    txn_manager_.CommitTransaction(txn);
    return res;
  }

  double ExecuteQuery(std::string query) {
    LOG_INFO("Execute query \n%s", query.c_str());
    std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
        new optimizer::Optimizer());
    const std::vector<type::Value> params;
    std::vector<StatementResult> result;
    const std::vector<int> result_format;
    executor::ExecuteResult p_status;
    auto *txn = txn_manager_.BeginTransaction();
    auto &peloton_parser = parser::PostgresParser::GetInstance();

    auto parsed_stmt = peloton_parser.BuildParseTree(query);

    auto plan = optimizer->BuildPelotonPlanTree(parsed_stmt, txn);

    LOG_DEBUG("%s", planner::PlanUtil::GetInfo(plan.get()).c_str());
    auto context = std::make_shared<executor::ExecutorContext>(txn);
    std::unique_ptr<executor::AbstractExecutor> executor_tree(
        executor::BuildExecutorTree(nullptr, plan.get(), context.get()));
    Timer<std::ratio<1, 1000>> timer;
    timer.Start();
    auto status = executor_tree->Init();
    if (status != true) {
      CleanExecutorTree(executor_tree.get());
      timer.Stop();
      return -1;
    }
    while (true) {
      if (!executor_tree->Execute())
        break;
      std::unique_ptr<executor::LogicalTile> tile(executor_tree->GetOutput());
      LOG_INFO("res=%s", tile->GetValue(0, 0).ToString().c_str());
    }
    CleanExecutorTree(executor_tree.get());
    timer.Stop();
    return timer.GetDuration();
  }

  concurrency::TransactionManager& txn_manager_;

  void PredicatePushDownPerformanceTest() {
    // Disable bloom filter in hash join
    settings::SettingsManager::SetBool(settings::SettingId::hash_join_bloom_filter, false);

    const size_t table1_tuple_size = 32;
    const size_t bigint_size = 8;

    std::vector<int> candidate_res = {};
    candidate_res = CreateAndLoadTable(bigint_size, table1_tuple_size, table1_table_size, 0.0, candidate_res);
    std::sort(candidate_res.begin(), candidate_res.end());
    settings::SettingsManager::SetBool(settings::SettingId::codegen, false);

    size_t index = candidate_res.size() * selectivity;
    // Hash on first, probe on second - codegen
    // Hash on the right column of the join condition
    std::string tables = "test1 as T0";
    for (int i=1; i<=num_join; i++) {
      auto alias = "T" + std::to_string(i);
      auto pre_alias = "T" + std::to_string(i-1);
      tables += "\njoin test2 as " + alias + " on " + alias + ".c1=" + pre_alias + ".c1";
    }
    std::string query = StringUtil::Format("SELECT count(T0.c1) FROM \n" + tables + "\nWHERE T0.c0 < %d", candidate_res[index]);
    settings::SettingsManager::SetBool(settings::SettingId::predicate_push_down, !reversed);
//    ExecuteQuery(query);
    double run_time1 = 0;
    for (int i=0; i<iter; i++) {
      auto tt = ExecuteQuery(query);
      LOG_INFO("%fms", tt);
      run_time1 += tt;
    }
    run_time1 = run_time1 / iter;

    settings::SettingsManager::SetBool(settings::SettingId::predicate_push_down, reversed);
//    ExecuteQuery(query);
    double run_time2 = 0;
    for (int i=0; i<iter; i++) {
      auto tt = ExecuteQuery(query);
      LOG_INFO("%fms", tt);
      run_time2 += tt;
    }
    run_time2 = run_time2 / iter;
    LOG_INFO("Run time with predicate push-down: %fms", run_time1);
    LOG_INFO("Run time without predicate push-down: %fms", run_time2);
  }

};
}  // namespace performance
}  // namespace peloton

// Entry point
int main(int argc, char **argv) {
  // Parse arguments
  peloton::performance::ParseArguments(argc, argv);
  peloton::performance::PrintConfig();

  // Run workload
  auto instance = new peloton::performance::OptimizerTests();
  instance->PredicatePushDownPerformanceTest();

  return 0;
}