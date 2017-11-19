//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_database.cpp
//
// Identification: src/main/tpch/tpch_database.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <fcntl.h>
#include <include/storage/storage_manager.h>
#include <include/type/ephemeral_pool.h>
#include <include/type/value_factory.h>
#include <include/catalog/table_catalog.h>
#include <include/catalog/column_catalog.h>

#include "concurrency/transaction_manager_factory.h"
#include "benchmark/tpch/tpch_database.h"

#include "benchmark/tpch/tpch_workload.h"
#include "catalog/catalog.h"
#include "storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace tpch {

// The TPCH database ID and each of the tables it owns
static oid_t kTPCHDatabaseId = 44;

//===----------------------------------------------------------------------===//
// Utility function to run a function over every line in the given file
//===----------------------------------------------------------------------===//
void ForEachLine(const std::string fname, std::function<void(char *)> cb) {
  // Open the file and advise the kernel of sequential access pattern
  int input_fd = open(fname.c_str(), O_RDONLY);

  const uint32_t BUFFER_SIZE = 16 * 1024;
  char buffer[BUFFER_SIZE] = {0};
  char *buf_pos = buffer;

  size_t bytes_to_read = BUFFER_SIZE;
  ssize_t bytes_read;
  while ((bytes_read = read(input_fd, buf_pos, bytes_to_read)) != 0) {
    if (bytes_read == -1) {
      perror("Error reading from input file");
      close(input_fd);
      exit(errno);
    }

    char *end = buf_pos + bytes_read;

    char *line_start = buffer;
    char *next_line_start;
    while ((next_line_start = strchr(line_start, '\n')) != nullptr &&
           next_line_start < end) {
      // Invoke callback on start of line
      cb(line_start);

      if (next_line_start == end - 1) {
        line_start = end;
        break;
      }

      line_start = next_line_start + 1;
    }

    bytes_to_read = BUFFER_SIZE;
    buf_pos = buffer;

    size_t tail_size = end - line_start;
    if (tail_size > 0) {
      PL_MEMCPY(buffer, line_start, tail_size);
      buf_pos = buffer + tail_size;
      bytes_to_read -= tail_size;
    }
  }
  close(input_fd);
}

// Convert the given string into a i32 date
uint32_t ConvertDate(char *p) {
  std::tm t_shipdate;
  PL_MEMSET(&t_shipdate, 0, sizeof(std::tm));
  strptime(p, "%Y-%m-%d", &t_shipdate);
  t_shipdate.tm_isdst = -1;
  return static_cast<uint32_t>(mktime(&t_shipdate));
}

//===----------------------------------------------------------------------===//
// TPCH DATABASE
//===----------------------------------------------------------------------===//

TPCHDatabase::TPCHDatabase(const Configuration &c) : config_(c) {
  // Create database instance
//  auto *database = new storage::Database(kTPCHDatabaseId);
//  database->setDBName("tpch");

  // Add databse instance to catalog
//  catalog::Catalog::GetInstance()->AddDatabase(database);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  kTPCHDatabaseId = catalog::Catalog::GetInstance()->GetDatabaseWithName(DEFAULT_DB_NAME, txn)->GetOid();
  txn_manager.CommitTransaction(txn);
  // Create all the test table
  CreateTables();

  // Mark all tables as not loaded
  for (uint32_t i = 0; i < 8; i++) {
    loaded_tables_[i] = false;
  }
}

TPCHDatabase::~TPCHDatabase() {
//  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto *txn = txn_manager.BeginTransaction();
//  catalog::Catalog::GetInstance()->DropDatabaseWithOid(kTPCHDatabaseId, txn);
//  txn_manager.CommitTransaction(txn);
}

// Create all the TPCH tables
void TPCHDatabase::CreateTables() const {
  // Create all table instances
  CreateCustomerTable();
  CreateLineitemTable();
  CreateNationTable();
  CreateOrdersTable();
  CreatePartTable();
  CreatePartSupplierTable();
  CreateRegionTable();
  CreateSupplierTable();
}

void TPCHDatabase::CreateTable(storage::DataTable* dt, std::vector<catalog::Column> columns) const {
  GetDatabase().AddTable(dt);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();
//  catalog::Catalog::GetInstance()->CreateTable(DEFAULT_DB_NAME, "customer", std::move(customer_schema), txn, false);
  std::unique_ptr<type::AbstractPool> pool{new type::EphemeralPool()};

  catalog::TableCatalog::GetInstance()->InsertTable(dt->GetOid(), dt->GetName(), GetDatabase().GetOid(),
                                                    pool.get(), txn);

  oid_t column_id = 0;
  for (const auto &column : columns) {
    catalog::ColumnCatalog::GetInstance()->InsertColumn(
        dt->GetOid(), column.GetName(), column_id, column.GetOffset(),
        column.GetType(), column.IsInlined(), column.GetConstraints(),
        pool.get(), txn);
    column_id++;

    // Create index on unique single column
    if (column.IsUnique()) {
      std::string col_name = column.GetName();
      std::string index_name = dt->GetName() + "_" + col_name + "_UNIQ";
      catalog::Catalog::GetInstance()->CreateIndex(DEFAULT_DB_NAME, dt->GetName(), {column_id}, index_name, true,
                                                   IndexType::BWTREE, txn);
      LOG_DEBUG("Added a UNIQUE index on %s in %s.", col_name.c_str(),
                column.GetName().c_str());
    }
  }

  catalog::Catalog::GetInstance()->CreatePrimaryIndex(GetDatabase().GetOid(), dt->GetOid(), txn);

  txn_manager.CommitTransaction(txn);
}

storage::Database &TPCHDatabase::GetDatabase() const {
  auto store_manager = storage::StorageManager::GetInstance();
  return *(store_manager->GetDatabaseWithOid(kTPCHDatabaseId));
}

storage::DataTable &TPCHDatabase::GetTable(TableId table_id) const {
  return *GetDatabase().GetTableWithOid(static_cast<uint32_t>(table_id));
}

uint32_t TPCHDatabase::DictionaryEncode(Dictionary &dict,
                                        const std::string &val) {
  auto iter = dict.find(val);
  if (iter == dict.end()) {
    uint32_t code = dict.size();
    dict.insert(std::make_pair(val, code));
    return code;
  } else {
    return iter->second;
  }
}

uint32_t TPCHDatabase::CodeForMktSegment(const std::string mktsegment) const {
  auto iter = c_mktsegment_dict_.find(mktsegment);
  return iter != c_mktsegment_dict_.end() ? iter->second : 0;
}

//===----------------------------------------------------------------------===//
// TABLE CREATORS
//===----------------------------------------------------------------------===//

void TPCHDatabase::CreateCustomerTable() const {
  catalog::Column c_custkey = {type::TypeId::INTEGER, kIntSize,
                               "c_custkey", true};
  catalog::Column c_name = {type::TypeId::VARCHAR, 64, "c_name", false};
  catalog::Column c_address = {type::TypeId::VARCHAR, 40, "c_address",
                               false};
  catalog::Column c_nationkey = {type::TypeId::INTEGER, kIntSize,
                                 "c_nationkey", true};
  catalog::Column c_phone = {type::TypeId::VARCHAR, 32, "c_phone", false};
  catalog::Column c_acctbal = {type::TypeId::DECIMAL, kDecimalSize,
                               "c_acctbal", true};
  catalog::Column c_mktsegment;
  if (config_.dictionary_encode) {
    c_mktsegment = {type::TypeId::INTEGER, kIntSize, "c_mktsegment",
                    true};
  } else {
    c_mktsegment = {type::TypeId::VARCHAR, 10, "c_mktsegment", true};
  }
  catalog::Column c_comment = {type::TypeId::VARCHAR, 117, "c_comment",
                               false};

  auto customer_cols = {c_custkey, c_name,    c_address,    c_nationkey,
                        c_phone,   c_acctbal, c_mktsegment, c_comment};

  // Create the schema
  std::unique_ptr<catalog::Schema> customer_schema{
      new catalog::Schema{customer_cols}};

  // Create the table!
  bool owns_schema = true;
  bool adapt_table = true;
  storage::DataTable *customer_table = storage::TableFactory::GetDataTable(
      kTPCHDatabaseId, (uint32_t)TableId::Customer, customer_schema.release(),
      "customer", config_.tuples_per_tile_group, owns_schema, adapt_table);

  // Add the table to the database (we're releasing ownership at this point)
  CreateTable(customer_table, customer_cols);
}

void TPCHDatabase::CreateLineitemTable() const {
  // Define columns
  catalog::Column l_orderkey = {type::TypeId::INTEGER, kIntSize, "l_orderkey"};
  catalog::Column l_partkey = {type::TypeId::INTEGER, kIntSize, "l_partkey"};
  catalog::Column l_suppkey = {type::TypeId::INTEGER, kIntSize, "l_suppkey"};
  catalog::Column l_linenumber = {type::TypeId::INTEGER, kIntSize, "l_linenumber"};
  catalog::Column l_quantity = {type::TypeId::INTEGER, kIntSize, "l_quantity"};
  catalog::Column l_extendedprice = {type::TypeId::DECIMAL, kDecimalSize, "l_extendedprice"};
  catalog::Column l_discount = {type::TypeId::DECIMAL, kDecimalSize, "l_discount"};
  catalog::Column l_tax = {type::TypeId::DECIMAL, kDecimalSize, "l_tax"};

  catalog::Column l_returnflag;
  if (config_.dictionary_encode) {
    l_returnflag = {type::TypeId::INTEGER, kIntSize, "l_returnflag"};
  } else {
    l_returnflag = {type::TypeId::VARCHAR, 1, "l_returnflag"};
  };

  catalog::Column l_linestatus;
  if (config_.dictionary_encode) {
    l_linestatus = {type::TypeId::INTEGER, kIntSize, "l_linestatus"};
  } else {
    l_linestatus = {type::TypeId::VARCHAR, 1, "l_linestatus"};
  }

  catalog::Column l_shipdate = {type::TypeId::DATE, kDateSize, "l_shipdate"};
  catalog::Column l_commitdate = {type::TypeId::DATE, kDateSize, "l_commitdate"};
  catalog::Column l_receiptdate = {type::TypeId::DATE, kDateSize, "l_receiptdate"};
  catalog::Column l_shipinstruct;
  if (config_.dictionary_encode) {
    l_shipinstruct = {type::TypeId::INTEGER, kIntSize, "l_shipinstruct"};
  } else {
    l_shipinstruct = {type::TypeId::VARCHAR, 25, "l_shipinstruct"};
  }
  catalog::Column l_shipmode;
  if (config_.dictionary_encode) {
    l_shipmode = {type::TypeId::INTEGER, kIntSize, "l_shipmode"};
  } else {
    l_shipmode = {type::TypeId::VARCHAR, 10, "l_shipmode"};
  }
  catalog::Column l_comment = {type::TypeId::VARCHAR, 44, "l_comment"};

  auto lineitem_cols = {
      l_orderkey,    l_partkey,       l_suppkey,  l_linenumber,
      l_quantity,    l_extendedprice, l_discount, l_tax,
      l_returnflag,  l_linestatus,    l_shipdate, l_commitdate,
      l_receiptdate, l_shipinstruct,  l_shipmode, l_comment};

  // Create the schema
  std::unique_ptr<catalog::Schema> lineitem_schema{
      new catalog::Schema{lineitem_cols}};

  // Create the table!
  bool owns_schema = true;
  bool adapt_table = true;
  storage::DataTable *lineitem_table = storage::TableFactory::GetDataTable(
      kTPCHDatabaseId, (uint32_t)TableId::Lineitem, lineitem_schema.release(),
      "lineitem", config_.tuples_per_tile_group, owns_schema, adapt_table, false);

  // Add the table to the database (we're releasing ownership at this point)
  CreateTable(lineitem_table, lineitem_cols);

}

void TPCHDatabase::CreateNationTable() const {
  catalog::Column n_nationkey = {type::TypeId::INTEGER, kIntSize,
                                 "n_nationkey", true};
  catalog::Column n_name = {type::TypeId::VARCHAR, 25, "n_name", false};
  catalog::Column n_regionKey = {type::TypeId::INTEGER, kIntSize, "n_regionkey",
                                 true};

  catalog::Column n_comment = {type::TypeId::VARCHAR, 152, "n_comment",
                               false};

  // Create the schema
  auto nation_cols = {n_nationkey, n_name, n_regionKey, n_comment};
  std::unique_ptr<catalog::Schema> nation_schema{new catalog::Schema{nation_cols}};

  // Create the table!
  bool owns_schema = true;
  bool adapt_table = true;
  storage::DataTable *nation_table = storage::TableFactory::GetDataTable(
      kTPCHDatabaseId, (uint32_t)TableId::Nation, nation_schema.release(), "nation",
      config_.tuples_per_tile_group, owns_schema, adapt_table, false);

  // Add the table to the database (we're releasing ownership at this point)
  CreateTable(nation_table, nation_cols);
}

void TPCHDatabase::CreateOrdersTable() const {
  catalog::Column o_orderkey = {type::TypeId::INTEGER, kIntSize,
                                "o_orderkey", true};
  catalog::Column o_custkey = {type::TypeId::INTEGER, kIntSize,
                               "o_custkey", true};
  catalog::Column o_orderstatus;
  if (config_.dictionary_encode) {
    o_orderstatus = {type::TypeId::INTEGER, kIntSize, "o_orderstatus"};
  } else {
    o_orderstatus = {type::TypeId::VARCHAR, 1, "o_orderstatus"};
  };

  catalog::Column o_totalprice = {type::TypeId::DECIMAL, kDecimalSize,
                                  "o_totalprice", true};
  catalog::Column o_orderdate = {type::TypeId::DATE, kDateSize,
                                 "o_orderdate", true};
  catalog::Column o_orderpriority = {type::TypeId::VARCHAR, 15,
                                     "o_orderpriority", false};
  catalog::Column o_clerk = {type::TypeId::VARCHAR, 15, "o_clerk", false};
  catalog::Column o_shippriority = {type::TypeId::INTEGER, kIntSize,
                                    "o_shippriority", true};
  catalog::Column o_comment = {type::TypeId::VARCHAR, 79, "o_comment",
                               true};

  // Create the schema
  auto orders_cols = {o_orderkey,   o_custkey,      o_orderstatus,
                      o_totalprice, o_orderdate,    o_orderpriority,
                      o_clerk,      o_shippriority, o_comment};
  std::unique_ptr<catalog::Schema> order_schema{
      new catalog::Schema{orders_cols}};

  // Create the table!
  bool owns_schema = true;
  bool adapt_table = true;
  storage::DataTable *order_table = storage::TableFactory::GetDataTable(
      kTPCHDatabaseId, (uint32_t)TableId::Orders, order_schema.release(),
      "orders", config_.tuples_per_tile_group, owns_schema, adapt_table, false);

  // Add the table to the database (we're releasing ownership at this point)
  CreateTable(order_table, orders_cols);

}

void TPCHDatabase::CreatePartTable() const {
  catalog::Column p_parkey = {type::TypeId::INTEGER, kIntSize,
                              "p_partkey", true};
  catalog::Column p_name = {type::TypeId::VARCHAR, 55, "p_name", false};
  catalog::Column p_mfgr = {type::TypeId::VARCHAR, 25, "p_mfgr", false};

  catalog::Column p_brand;
  if (config_.dictionary_encode) {
    p_brand = {type::TypeId::INTEGER, kIntSize, "p_brand", true};
  } else {
    p_brand = {type::TypeId::VARCHAR, 10, "p_brand", true};
  }

  catalog::Column p_type = {type::TypeId::VARCHAR, 25, "p_type", true};
  catalog::Column p_size = {type::TypeId::INTEGER, kIntSize, "p_size",
                            true};

  catalog::Column p_container;
  if (config_.dictionary_encode) {
    p_container = {type::TypeId::INTEGER, kIntSize, "p_container", true};
  } else {
    p_container = {type::TypeId::VARCHAR, 10, "p_container", true};
  }

  catalog::Column p_retailprice = {type::TypeId::DECIMAL, kDecimalSize,
                                   "p_retailprice", true};
  catalog::Column p_comment = {type::TypeId::VARCHAR, 23, "p_comment",
                               false};

  // Create the schema
  auto part_cols = {p_parkey, p_name,      p_mfgr,        p_brand,  p_type,
                    p_size,   p_container, p_retailprice, p_comment};
  std::unique_ptr<catalog::Schema> part_schema{new catalog::Schema{part_cols}};

  // Create the table!
  bool owns_schema = true;
  bool adapt_table = true;
  storage::DataTable *part_table = storage::TableFactory::GetDataTable(
      kTPCHDatabaseId, (uint32_t)TableId::Part, part_schema.release(), "part",
      config_.tuples_per_tile_group, owns_schema, adapt_table, false);

  // Add the table to the database (we're releasing ownership at this point)
  CreateTable(part_table, part_cols);
}

void TPCHDatabase::CreatePartSupplierTable() const {
  // Define columns
  catalog::Column ps_partkey = {type::TypeId::INTEGER, kIntSize, "ps_partkey"};
  catalog::Column ps_suppkey = {type::TypeId::INTEGER, kIntSize, "ps_suppkey"};
  catalog::Column ps_availqty = {type::TypeId::INTEGER, kIntSize, "ps_availqty"};
  catalog::Column ps_supplycost = {type::TypeId::DECIMAL, kDecimalSize, "ps_supplycost"};
  catalog::Column ps_comment = {type::TypeId::VARCHAR, 199, "ps_comment"};

  auto ps_cols = {
      ps_partkey,    ps_suppkey,       ps_availqty,  ps_supplycost,
      ps_comment};

  // Create the schema
  std::unique_ptr<catalog::Schema> ps_schema{
      new catalog::Schema{ps_cols}};

  // Create the table!
  bool owns_schema = true;
  bool adapt_table = true;
  storage::DataTable *ps_table = storage::TableFactory::GetDataTable(
      kTPCHDatabaseId, (uint32_t)TableId::PartSupp, ps_schema.release(),
      "partsupp", config_.tuples_per_tile_group, owns_schema, adapt_table, false);

  // Add the table to the database (we're releasing ownership at this point)
  CreateTable(ps_table, ps_cols);
}

void TPCHDatabase::CreateRegionTable() const {
  // Define columns
  catalog::Column r_regionkey = {type::TypeId::INTEGER, kIntSize, "r_regionkey"};
  catalog::Column r_name = {type::TypeId::VARCHAR, 25, "r_name"};
  catalog::Column r_comment = {type::TypeId::VARCHAR, 152, "r_comment"};

  auto r_cols = {
      r_regionkey,    r_name,       r_comment};

  // Create the schema
  std::unique_ptr<catalog::Schema> r_schema{
      new catalog::Schema{r_cols}};

  // Create the table!
  bool owns_schema = true;
  bool adapt_table = true;
  storage::DataTable *r_table = storage::TableFactory::GetDataTable(
      kTPCHDatabaseId, (uint32_t)TableId::Region, r_schema.release(),
      "region", config_.tuples_per_tile_group, owns_schema, adapt_table, false);

  // Add the table to the database (we're releasing ownership at this point)
  CreateTable(r_table, r_cols);
}

void TPCHDatabase::CreateSupplierTable() const {
  // Define columns
  catalog::Column s_suppkey = {type::TypeId::INTEGER, kIntSize, "s_suppkey"};
  catalog::Column s_name = {type::TypeId::VARCHAR, 25, "s_name"};
  catalog::Column s_address = {type::TypeId::VARCHAR, 40, "s_address"};
  catalog::Column s_nationkey = {type::TypeId::INTEGER, kIntSize, "s_nationkey"};
  catalog::Column s_phone = {type::TypeId::VARCHAR, 15, "s_phone"};
  catalog::Column s_acctbal = {type::TypeId::DECIMAL, kDecimalSize, "s_acctbal"};
  catalog::Column s_comment = {type::TypeId::VARCHAR, 101, "s_comment"};

  auto s_cols = {
      s_suppkey,    s_name,       s_address,  s_nationkey,
      s_phone, s_acctbal, s_comment};

  // Create the schema
  std::unique_ptr<catalog::Schema> s_schema{
      new catalog::Schema{s_cols}};

  // Create the table!
  bool owns_schema = true;
  bool adapt_table = true;
  storage::DataTable *s_table = storage::TableFactory::GetDataTable(
      kTPCHDatabaseId, (uint32_t)TableId::Supplier, s_schema.release(),
      "supplier", config_.tuples_per_tile_group, owns_schema, adapt_table, false);

  // Add the table to the database (we're releasing ownership at this point)
  CreateTable(s_table, s_cols);
}

//===----------------------------------------------------------------------===//
// TABLE LOADERS
//===----------------------------------------------------------------------===//

void TPCHDatabase::LoadTable(TableId table_id) {
  switch (table_id) {
    case TableId::Customer: LoadCustomerTable(); break;
    case TableId::Lineitem: LoadLineitemTable(); break;
    case TableId::Nation:   LoadNationTable(); break;
    case TableId::Orders:   LoadOrdersTable(); break;
    case TableId::Part:     LoadPartTable(); break;
    case TableId::PartSupp: LoadPartSupplierTable(); break;
    case TableId::Region:   LoadRegionTable(); break;
    case TableId::Supplier: LoadSupplierTable(); break;
  }
}

void TPCHDatabase::LoadPartTable() {
  if (TableIsLoaded(TableId::Part)) {
    return;
  }

  const std::string filename = config_.GetPartPath();

  LOG_INFO("Loading Part ['%s']\n", filename.c_str());

  auto &table = GetTable(TableId::Part);

  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  uint64_t num_tuples = 0;
  std::unique_ptr<type::AbstractPool> pool{new type::EphemeralPool()};

  ForEachLine(filename, [&](char *p){
    storage::Tuple tuple{table.GetSchema(), true /* allocate */};

    tuple.SetValue(0, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    char *p_end = strchr(p, '|');
    tuple.SetValue(1, type::ValueFactory::GetVarcharValue(std::string{p, p_end}), pool.get());

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(2, type::ValueFactory::GetVarcharValue(std::string{p, p_end}), pool.get());

    p = p_end + 1;
    p_end = strchr(p, '|');
    std::string p_brand{p, p_end};
    if (config_.dictionary_encode) {
      uint32_t code = DictionaryEncode(p_brand_dict_, p_brand);
      tuple.SetValue(3, type::ValueFactory::GetIntegerValue(code));
    } else {
      tuple.SetValue(3, type::ValueFactory::GetVarcharValue(p_brand), pool.get());
    }

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(4, type::ValueFactory::GetVarcharValue(std::string{p, p_end}), pool.get());

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(5, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = p_end + 1;
    p_end = strchr(p, '|');
    std::string p_container{p, p_end};
    if (config_.dictionary_encode) {
      uint32_t code = DictionaryEncode(p_container_dict_, p_container);
      tuple.SetValue(6, type::ValueFactory::GetIntegerValue(code));
    } else {
      tuple.SetValue(6, type::ValueFactory::GetVarcharValue(p_container), pool.get());
    }

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(7, type::ValueFactory::GetDecimalValue(std::atof(p)));

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(8, type::ValueFactory::GetVarcharValue(std::string{p, p_end}), pool.get());

    // Insert into table
    ItemPointer tuple_slot_id = table.InsertTuple(&tuple);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
    txn_manager.PerformInsert(txn, tuple_slot_id);

    num_tuples++;

  });

  // Commit
  auto res = txn_manager.CommitTransaction(txn);
  PL_ASSERT(res == ResultType::SUCCESS);
  if (res != ResultType::SUCCESS) {
    LOG_ERROR("Could not commit transaction during load!");
  }

  timer.Stop();
  LOG_INFO("Loading Part finished: %.2f ms (%lu tuples)\n",
           timer.GetDuration(), num_tuples);

  // Set table as loaded
  SetTableIsLoaded(TableId::Part);
}

void TPCHDatabase::LoadSupplierTable() {
  // Short-circuit if table is already loaded
  if (TableIsLoaded(TableId::Supplier)) {
    return;
  }

  const std::string filename = config_.GetSupplierPath();

  LOG_INFO("Loading Supplier ['%s']\n", filename.c_str());

  uint64_t num_tuples = 0;

  auto &table = GetTable(TableId::Supplier);

  std::unique_ptr<type::AbstractPool> pool{new type::EphemeralPool()};

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  ForEachLine(filename, [&](char *p) {
    // The input tuple
    storage::Tuple tuple{table.GetSchema(), /*allocate*/ true};

    tuple.SetValue(0, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    char *p_end = strchr(p, '|');
    tuple.SetValue(1, type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(2, type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(3, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(4, type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(5, type::ValueFactory::GetDecimalValue(std::atof(p)));

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(6, type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());



    // Insert into table
    ItemPointer tuple_slot_id = table.InsertTuple(&tuple);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
    txn_manager.PerformInsert(txn, tuple_slot_id);

    num_tuples++;
  });

  // Commit
  auto res = txn_manager.CommitTransaction(txn);
  PL_ASSERT(res == ResultType::SUCCESS);
  if (res != ResultType::SUCCESS) {
    LOG_ERROR("Could not commit transaction during load!");
  }

  timer.Stop();
  LOG_INFO("Loading Supplier finished: %.2f ms (%lu tuples)\n",
           timer.GetDuration(), num_tuples);

  // Set table as loaded
  SetTableIsLoaded(TableId::Supplier);
}

void TPCHDatabase::LoadPartSupplierTable() {
  // Short-circuit if table is already loaded
  if (TableIsLoaded(TableId::PartSupp)) {
    return;
  }

  const std::string filename = config_.GetPartSuppPath();

  LOG_INFO("Loading PartSupp ['%s']\n", filename.c_str());

  uint64_t num_tuples = 0;

  auto &table = GetTable(TableId::PartSupp);

  std::unique_ptr<type::AbstractPool> pool{new type::EphemeralPool()};

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  ForEachLine(filename, [&](char *p) {
    // The input tuple
    storage::Tuple tuple{table.GetSchema(), /*allocate*/ true};

    tuple.SetValue(0, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(1, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(2, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(3, type::ValueFactory::GetDecimalValue(std::atof(p)));

    p = strchr(p, '|') + 1;
    char *p_end = strchr(p, '|');
    tuple.SetValue(4, type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());

    // Insert into table
    ItemPointer tuple_slot_id = table.InsertTuple(&tuple);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
    txn_manager.PerformInsert(txn, tuple_slot_id);

    num_tuples++;
  });

  // Commit
  auto res = txn_manager.CommitTransaction(txn);
  PL_ASSERT(res == ResultType::SUCCESS);
  if (res != ResultType::SUCCESS) {
    LOG_ERROR("Could not commit transaction during load!");
  }

  timer.Stop();
  LOG_INFO("Loading PartSupp finished: %.2f ms (%lu tuples)\n",
           timer.GetDuration(), num_tuples);

  // Set table as loaded
  SetTableIsLoaded(TableId::PartSupp);
}

void TPCHDatabase::LoadCustomerTable() {
  if (TableIsLoaded(TableId::Customer)) {
    return;
  }

  const std::string filename = config_.GetCustomerPath();

  LOG_INFO("Loading Customer ['%s']\n", filename.c_str());

  auto &table = GetTable(TableId::Customer);

  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  uint64_t num_tuples = 0;
  std::unique_ptr<type::AbstractPool> pool{new type::EphemeralPool()};

  ForEachLine(filename, [&](char *p) {
    storage::Tuple tuple{table.GetSchema(), true /* allocate */};

    // C_CUSTKEY
    int32_t c_custkey = std::atoi(p);
    tuple.SetValue(0, type::ValueFactory::GetIntegerValue(c_custkey));

    // C_NAME
    p = strchr(p, '|') + 1;
    char *p_end = strchr(p, '|');

    std::string c_name{p, p_end};
    tuple.SetValue(1, type::ValueFactory::GetVarcharValue(c_name), pool.get());

    // C_ADDRESS
    p = p_end + 1;
    p_end = strchr(p, '|');

    std::string c_address{p, p_end};
    tuple.SetValue(2, type::ValueFactory::GetVarcharValue(c_address), pool.get());

    // C_NATIONKEY
    p = p_end + 1;

    int32_t c_nationkey = std::atoi(p);
    tuple.SetValue(3, type::ValueFactory::GetIntegerValue(c_nationkey));

    // C_PHONE
    p = strchr(p, '|') + 1;
    p_end = strchr(p, '|');
    std::string c_phone{p, p_end};
    tuple.SetValue(4, type::ValueFactory::GetVarcharValue(c_phone), pool.get());

    // C_ACCTBA
    p = p_end + 1;
    double c_acctba = std::atof(p);
    tuple.SetValue(5, type::ValueFactory::GetDecimalValue(c_acctba));

    // C_MKTSEGMENT
    p = strchr(p, '|') + 1;
    p_end = strchr(p, '|');
    std::string c_mktsegment{p, p_end};
    if (config_.dictionary_encode) {
      uint32_t code = DictionaryEncode(c_mktsegment_dict_, c_mktsegment);
      tuple.SetValue(6, type::ValueFactory::GetIntegerValue(code));
    } else {
      tuple.SetValue(6, type::ValueFactory::GetVarcharValue(c_mktsegment), pool.get());
    }

    // C_COMMENT
    p = p_end + 1;
    p_end = strchr(p, '|');
    std::string c_comment{p, p_end};
    tuple.SetValue(7, type::ValueFactory::GetVarcharValue(c_comment), pool.get());

    // Insert into table
    ItemPointer tuple_slot_id = table.InsertTuple(&tuple);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
    txn_manager.PerformInsert(txn, tuple_slot_id);

    num_tuples++;
  });

  // Commit
  auto res = txn_manager.CommitTransaction(txn);
  PL_ASSERT(res == ResultType::SUCCESS);
  if (res != ResultType::SUCCESS) {
    LOG_ERROR("Could not commit transaction during load!");
  }

  timer.Stop();
  LOG_INFO("Loading Customer finished: %.2f ms (%lu tuples)\n",
           timer.GetDuration(), num_tuples);

  // Set table as loaded
  SetTableIsLoaded(TableId::Customer);
}

void TPCHDatabase::LoadNationTable() {
  if (TableIsLoaded(TableId::Nation)) {
    return;
  }

  const std::string filename = config_.GetNationPath();

  LOG_INFO("Loading Nation ['%s']\n", filename.c_str());

  auto &table = GetTable(TableId::Nation);

  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  uint64_t num_tuples = 0;
  std::unique_ptr<type::AbstractPool> pool{new type::EphemeralPool()};

  ForEachLine(filename, [&](char *p){
    storage::Tuple tuple{table.GetSchema(), true /* allocate */};

    tuple.SetValue(0, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    char *p_end = strchr(p, '|');
    tuple.SetValue(1, type::ValueFactory::GetVarcharValue(std::string{p, p_end}), pool.get());

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(2, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(3, type::ValueFactory::GetVarcharValue(std::string{p, p_end}), pool.get());

    // Insert into table
    ItemPointer tuple_slot_id = table.InsertTuple(&tuple);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
    txn_manager.PerformInsert(txn, tuple_slot_id);

    num_tuples++;

  });

  // Commit
  auto res = txn_manager.CommitTransaction(txn);
  PL_ASSERT(res == ResultType::SUCCESS);
  if (res != ResultType::SUCCESS) {
    LOG_ERROR("Could not commit transaction during load!");
  }

  timer.Stop();
  LOG_INFO("Loading Nation finished: %.2f ms (%lu tuples)\n",
           timer.GetDuration(), num_tuples);

  // Set table as loaded
  SetTableIsLoaded(TableId::Nation);
}

void TPCHDatabase::LoadLineitemTable() {
  // Short-circuit if table is already loaded
  if (TableIsLoaded(TableId::Lineitem)) {
    return;
  }

  const std::string filename = config_.GetLineitemPath();

  LOG_INFO("Loading Lineitem ['%s']\n", filename.c_str());

  uint64_t num_tuples = 0;

  auto &table = GetTable(TableId::Lineitem);

  std::unique_ptr<type::AbstractPool> pool{new type::EphemeralPool()};

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  ForEachLine(filename, [&](char *p) {
    // The input tuple
    storage::Tuple tuple{table.GetSchema(), /*allocate*/ true};

    tuple.SetValue(0, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(1, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(2, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(3, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(4,
                   type::ValueFactory::GetIntegerValue((int32_t)std::atof(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(5, type::ValueFactory::GetDecimalValue(std::atof(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(6, type::ValueFactory::GetDecimalValue(std::atof(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(7, type::ValueFactory::GetDecimalValue(std::atof(p)));

    p = strchr(p, '|') + 1;
    char returnflag = *p;
    if (config_.dictionary_encode) {
      tuple.SetValue(8, type::ValueFactory::GetIntegerValue(returnflag));
    } else {
      tuple.SetValue(8, type::ValueFactory::GetVarcharValue(std::string(1, returnflag)), pool.get());
    }

    p = strchr(p, '|') + 1;
    char linestatus = *p;
    if (config_.dictionary_encode) {
      tuple.SetValue(9, type::ValueFactory::GetIntegerValue(linestatus));
    } else {
      tuple.SetValue(9, type::ValueFactory::GetVarcharValue(std::string(1, linestatus)), pool.get());
    }

    p = strchr(p, '|') + 1;
    tuple.SetValue(10, type::ValueFactory::GetDateValue(ConvertDate(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(11, type::ValueFactory::GetDateValue(ConvertDate(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(12, type::ValueFactory::GetDateValue(ConvertDate(p)));

    p = strchr(p, '|') + 1;
    char *p_end = strchr(p, '|');
    std::string l_shipinstruct{p, p_end};
    if (config_.dictionary_encode) {
      uint32_t code = DictionaryEncode(l_shipinstruct_dict_, l_shipinstruct);
      tuple.SetValue(13, type::ValueFactory::GetIntegerValue(code), nullptr);
    } else {
      tuple.SetValue(13, type::ValueFactory::GetVarcharValue(l_shipinstruct),
                     pool.get());
    }

    p = p_end + 1;
    p_end = strchr(p, '|');
    std::string l_shipmode{p, p_end};
    if (config_.dictionary_encode) {
      uint32_t code = DictionaryEncode(l_shipmode_dict_, l_shipmode);
      tuple.SetValue(14, type::ValueFactory::GetIntegerValue(code));
    } else {
      tuple.SetValue(14, type::ValueFactory::GetVarcharValue(l_shipmode),
                     pool.get());
    }

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(15,
                   type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());

    // Insert into table
    ItemPointer tuple_slot_id = table.InsertTuple(&tuple);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
    txn_manager.PerformInsert(txn, tuple_slot_id);

    num_tuples++;
  });

  // Commit
  auto res = txn_manager.CommitTransaction(txn);
  PL_ASSERT(res == ResultType::SUCCESS);
  if (res != ResultType::SUCCESS) {
    LOG_ERROR("Could not commit transaction during load!");
  }

  timer.Stop();
  LOG_INFO("Loading Lineitem finished: %.2f ms (%lu tuples)\n",
           timer.GetDuration(), num_tuples);

  // Set table as loaded
  SetTableIsLoaded(TableId::Lineitem);
}

void TPCHDatabase::LoadRegionTable() {
  // Short-circuit if table is already loaded
  if (TableIsLoaded(TableId::Region)) {
    return;
  }

  const std::string filename = config_.GetRegionPath();

  LOG_INFO("Loading Region ['%s']\n", filename.c_str());

  uint64_t num_tuples = 0;

  auto &table = GetTable(TableId::Region);

  std::unique_ptr<type::AbstractPool> pool{new type::EphemeralPool()};

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  ForEachLine(filename, [&](char *p) {
    // The input tuple
    storage::Tuple tuple{table.GetSchema(), /*allocate*/ true};

    tuple.SetValue(0, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    char *p_end = strchr(p, '|');
    tuple.SetValue(1, type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(2, type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());


    // Insert into table
    ItemPointer tuple_slot_id = table.InsertTuple(&tuple);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
    txn_manager.PerformInsert(txn, tuple_slot_id);

    num_tuples++;
  });

  // Commit
  auto res = txn_manager.CommitTransaction(txn);
  PL_ASSERT(res == ResultType::SUCCESS);
  if (res != ResultType::SUCCESS) {
    LOG_ERROR("Could not commit transaction during load!");
  }

  timer.Stop();
  LOG_INFO("Loading Region finished: %.2f ms (%lu tuples)\n",
           timer.GetDuration(), num_tuples);

  // Set table as loaded
  SetTableIsLoaded(TableId::Region);
}

void TPCHDatabase::LoadOrdersTable() {
  // Short-circuit if table is already loaded
  if (TableIsLoaded(TableId::Orders)) {
    return;
  }

  const std::string filename = config_.GetOrdersPath();

  LOG_INFO("Loading Orders ['%s']\n", filename.c_str());

  uint64_t num_tuples = 0;

  auto &table = GetTable(TableId::Orders);

  std::unique_ptr<type::AbstractPool> pool{new type::EphemeralPool()};

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  ForEachLine(filename, [&](char *p) {
    // The input tuple
    storage::Tuple tuple{table.GetSchema(), /*allocate*/ true};

    tuple.SetValue(0, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(1, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(2, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(3, type::ValueFactory::GetDecimalValue(std::atof(p)));

    p = strchr(p, '|') + 1;
    tuple.SetValue(4,
                   type::ValueFactory::GetDateValue(ConvertDate(p)));

    p = strchr(p, '|') + 1;
    char *p_end = strchr(p, '|');
    tuple.SetValue(5, type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(6, type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(7, type::ValueFactory::GetIntegerValue(std::atoi(p)));

    p = p_end + 1;
    p_end = strchr(p, '|');
    tuple.SetValue(8,
                   type::ValueFactory::GetVarcharValue(std::string{p, p_end}),
                   pool.get());

    // Insert into table
    ItemPointer tuple_slot_id = table.InsertTuple(&tuple);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
    txn_manager.PerformInsert(txn, tuple_slot_id);

    num_tuples++;
  });

  // Commit
  auto res = txn_manager.CommitTransaction(txn);
  PL_ASSERT(res == ResultType::SUCCESS);
  if (res != ResultType::SUCCESS) {
    LOG_ERROR("Could not commit transaction during load!");
  }

  timer.Stop();
  LOG_INFO("Loading Orders finished: %.2f ms (%lu tuples)\n",
           timer.GetDuration(), num_tuples);

  // Set table as loaded
  SetTableIsLoaded(TableId::Orders);
}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton
