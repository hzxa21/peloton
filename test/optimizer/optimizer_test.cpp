#include <include/settings/settings_manager.h>
#include "common/harness.h"

#define private public

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

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Binding Tests
//===--------------------------------------------------------------------===//

using namespace optimizer;

class OptimizerTests : public PelotonTest {
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

  std::vector<int> CreateAndLoadTable(std::string table_name,
                                      size_t type_size,
                                      size_t tuple_size,
                                      size_t table_size,
                                      double join_selectivity,
                                      std::vector<int>& candidate_res) {
    LOG_INFO("Create table %s", table_name.c_str());
    auto *txn = txn_manager_.BeginTransaction();
    TestingSQLUtil::CreateTable(table_name, tuple_size, txn);
    LOG_INFO("Load data into table %s, tuple_size = %ld, table_size = %ld",
             table_name.c_str(), tuple_size, table_size);
    std::vector<int> res;
    std::unordered_set<int> res_set;
    size_t curr_size = 0;
    size_t val_size = (tuple_size + type_size - 1) / type_size;
    auto *table = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, table_name, txn);
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

      // Insert tuple into the table
      std::vector<int> vals(val_size, random);
      TestingSQLUtil::InsertTuple(vals, table, txn);

      curr_size += tuple_size;
    }

    txn_manager_.CommitTransaction(txn);
    return res;
  }

  double ExecuteQuery(std::string query, std::vector<StatementResult> &result) {
    LOG_INFO("Execute query %s", query.c_str());
    Timer<std::ratio<1, 1000>> timer;
    timer.Start();
    TestingSQLUtil::ExecuteSQLQuery(query, result);
    timer.Stop();
    return timer.GetDuration();
  }

  concurrency::TransactionManager& txn_manager_;


};

// Test whether update stament will use index scan plan
// TODO: Split the tests into separate test cases.
TEST_F(OptimizerTests, HashJoinTest) {
  optimizer::Optimizer optimizer;
  auto& traffic_cop = tcop::TrafficCop::GetInstance();
  traffic_cop.SetTaskCallback(TestingSQLUtil::UtilTestTaskCallback, &TestingSQLUtil::counter_);

  // Create a table first
  auto txn = txn_manager_.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Creating table");
  LOG_INFO("Query: CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);");
  std::unique_ptr<Statement> statement;
  statement.reset(new Statement(
      "CREATE", "CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);"));

  auto& peloton_parser = parser::PostgresParser::GetInstance();

  auto create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE table_a(aid INT PRIMARY KEY,value INT);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(create_stmt, txn));

  std::vector<type::Value> params;
  std::vector<StatementResult> result;
  std::vector<int> result_format;
  result_format =
      std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  TestingSQLUtil::counter_.store(1);
  executor::ExecuteResult status = traffic_cop.ExecuteStatementPlan(
      statement->GetPlanTree(), params, result, result_format);
  if (traffic_cop.is_queuing_) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.is_queuing_ = false;
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Table Created");
  traffic_cop.CommitQueryHelper();

  txn = txn_manager_.BeginTransaction();
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME, txn)
                ->GetTableCount(),
            1);

  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Creating table");
  LOG_INFO("Query: CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);");
  statement.reset(new Statement(
      "CREATE", "CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);"));

  create_stmt = peloton_parser.BuildParseTree(
      "CREATE TABLE table_b(bid INT PRIMARY KEY,value INT);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(create_stmt, txn));

  result_format =
      std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree(),
                                            params, result, result_format);
  if (traffic_cop.is_queuing_) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.is_queuing_ = false;
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Table Created");
  traffic_cop.CommitQueryHelper();

  txn = txn_manager_.BeginTransaction();
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName(DEFAULT_DB_NAME, txn)
                ->GetTableCount(),
            2);

  // Inserting a tuple to table_a
  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Inserting a tuple...");
  LOG_INFO("Query: INSERT INTO table_a(aid, value) VALUES (1,1);");
  statement.reset(new Statement(
      "INSERT", "INSERT INTO table_a(aid, value) VALUES (1, 1);"));

  auto insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_a(aid, value) VALUES (1, 1);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt, txn));

  result_format =
      std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree(),
                                            params, result, result_format);
  if (traffic_cop.is_queuing_) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.is_queuing_ = false;
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted to table_a!");
  traffic_cop.CommitQueryHelper();

  // Inserting a tuple to table_b
  txn = txn_manager_.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Inserting a tuple...");
  LOG_INFO("Query: INSERT INTO table_b(bid, value) VALUES (1,2);");
  statement.reset(new Statement(
      "INSERT", "INSERT INTO table_b(bid, value) VALUES (1, 2);"));

  insert_stmt = peloton_parser.BuildParseTree(
      "INSERT INTO table_b(bid, value) VALUES (1, 2);");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(insert_stmt, txn));

  result_format =
      std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree(),
                                            params, result, result_format);
  if (traffic_cop.is_queuing_) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.is_queuing_ = false;
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Tuple inserted to table_b!");
  traffic_cop.CommitQueryHelper();

  txn = txn_manager_.BeginTransaction();
  traffic_cop.SetTcopTxnState(txn);
  LOG_INFO("Join ...");
  LOG_INFO("Query: SELECT * FROM table_a INNER JOIN table_b ON aid = bid;");
  statement.reset(new Statement(
      "SELECT", "SELECT * FROM table_a INNER JOIN table_b ON aid = bid;"));

  auto select_stmt = peloton_parser.BuildParseTree(
      "SELECT * FROM table_a INNER JOIN table_b ON aid = bid;");

  statement->SetPlanTree(optimizer.BuildPelotonPlanTree(select_stmt, txn));

  result_format = std::vector<int>(4, 0);
  TestingSQLUtil::counter_.store(1);
  status = traffic_cop.ExecuteStatementPlan(statement->GetPlanTree(),
                                            params, result, result_format);
  if (traffic_cop.is_queuing_) {
    TestingSQLUtil::ContinueAfterComplete();
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.p_status_;
    traffic_cop.is_queuing_ = false;
  }
  LOG_INFO("Statement executed. Result: %s",
           ResultTypeToString(status.m_result).c_str());
  LOG_INFO("Join completed!");
  traffic_cop.CommitQueryHelper();
}

TEST_F(OptimizerTests, PredicatePushDownTest) {
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);");
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE test1(a INT PRIMARY KEY, b INT, c INT);");

  auto& peloton_parser = parser::PostgresParser::GetInstance();
  auto stmt = peloton_parser.BuildParseTree(
      "SELECT * FROM test, test1 WHERE test.a = test1.a AND test1.b = 22");

  optimizer::Optimizer optimizer;
  auto txn = txn_manager_.BeginTransaction();
  auto plan = optimizer.BuildPelotonPlanTree(stmt, txn);
  txn_manager_.CommitTransaction(txn);


  auto& child_plan = plan->GetChildren();
  EXPECT_EQ(2, child_plan.size());


  auto l_plan = dynamic_cast<planner::SeqScanPlan*>(child_plan[0].get());
  auto r_plan = dynamic_cast<planner::SeqScanPlan*>(child_plan[1]->GetChildren()[0].get());
  planner::SeqScanPlan* test_plan = l_plan;
  planner::SeqScanPlan* test1_plan = r_plan;

  if (l_plan->GetTable()->GetName() == "test1") {
    test_plan = r_plan;
    test1_plan = l_plan;
  }

  auto test_predicate = test_plan->GetPredicate();
  EXPECT_EQ(nullptr, test_predicate);
  auto test1_predicate = test1_plan->GetPredicate();
  EXPECT_EQ(ExpressionType::COMPARE_EQUAL, test1_predicate->GetExpressionType());
  auto tv = dynamic_cast<expression::TupleValueExpression*>(test1_predicate->GetModifiableChild(0));
  EXPECT_TRUE(tv != nullptr);
  EXPECT_EQ("test1", tv->GetTableName());
  EXPECT_EQ("b", tv->GetColumnName());
  auto constant = dynamic_cast<expression::ConstantValueExpression*>(test1_predicate->GetModifiableChild(1));
  EXPECT_TRUE(constant != nullptr);
  EXPECT_EQ(22, constant->GetValue().GetAs<int>());
}

TEST_F(OptimizerTests, PredicatePushDownPerformanceTest) {
  // Disable bloom filter in hash join
  settings::SettingsManager::SetBool(settings::SettingId::hash_join_bloom_filter, false);

  // Initialize tables. test1 is the inner table from which we build the
  // hash table. test2 is the outer table which will probe the hash table.
  const std::string table1_name = "test1";
  const size_t table1_tuple_size = 32;
  const size_t table1_table_size = 100000000;
  const size_t bigint_size = 8;
  double selectivity = 0.6;
  int iter = 3;

  std::vector<int> candidate_res = {};
  candidate_res = CreateAndLoadTable(table1_name, bigint_size, table1_tuple_size, table1_table_size, 0.0, candidate_res);
  std::sort(candidate_res.begin(), candidate_res.end());
  settings::SettingsManager::SetBool(settings::SettingId::codegen, false);

  size_t index = candidate_res.size() * selectivity;
  // Hash on first, probe on second - codegen
  // Hash on the right column of the join condition
  std::string query = StringUtil::Format("SELECT count(T1.c0) FROM test1 as T1 join test1 as T2 on T1.c0 = T2.c0 WHERE T1.c0 < %d", candidate_res[index]);
  std::vector<StatementResult> result1, result2;
  ExecuteQuery(query, result1);
  double run_time1 = 0;
  for (int i=0; i<iter; i++) {
    auto tt = ExecuteQuery(query, result1);
    LOG_INFO("%fms", tt);
    run_time1 += ExecuteQuery(query, result1);
  }
  run_time1 = run_time1 / iter;

  settings::SettingsManager::SetBool(settings::SettingId::predicate_push_down, false);
  ExecuteQuery(query, result2);
  double run_time2 = 0;
  for (int i=0; i<iter; i++) {
    auto tt = ExecuteQuery(query, result2);
    LOG_INFO("%fms", tt);
    run_time2 += ExecuteQuery(query, result2);
  }
  run_time2 = run_time2 / iter;
  LOG_INFO("Run time with predicate push-down: %fms", run_time1);
  LOG_INFO("Run time without predicate push-down: %fms", run_time2);
  EXPECT_EQ(result1, result2);
}

}  // namespace test
}  // namespace peloton
