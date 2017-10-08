//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// binder_context.cpp
//
// Identification: src/binder/binder_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "binder/binder_context.h"

#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/table_catalog.h"
#include "parser/table_ref.h"

namespace peloton {
namespace binder {

void BinderContext::AddTable(const parser::TableRef* table_ref,
                             concurrency::Transaction* txn) {
  auto table_alias = table_ref->GetTableAlias();
  if (table_alias == nullptr)
    table_alias = table_ref->GetTableName();
  AddTable(table_ref->GetDatabaseName(), table_ref->GetTableName(), table_alias, txn);
}

void BinderContext::AddTable(const std::string db_name,
                             const std::string table_name,
                             const std::string table_alias,
                             concurrency::Transaction* txn) {
  // using catalog object to retrieve meta-data
  auto table_object =
      catalog::Catalog::GetInstance()->GetTableObject(db_name, table_name, txn);

  if (table_alias_map.find(table_alias) != table_alias_map.end()) {
    throw Exception("Duplicate alias " + table_alias);
  }
  table_alias_map[table_alias] = table_object;
}

bool BinderContext::GetColumnPosTuple(
    const std::string& col_name, std::shared_ptr<catalog::TableCatalogObject> table_obj,
    std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple, type::TypeId& value_type) {
  try {
    auto column_object = table_obj->GetColumnObject(col_name);
    if (column_object == nullptr) return false;

    oid_t col_pos = column_object->column_id;
    col_pos_tuple = std::make_tuple(table_obj->database_oid, table_obj->table_oid, col_pos);
    value_type = column_object->column_type;
    return true;
  } catch (CatalogException& e) {
    LOG_TRACE("Can't find table %d! Return false", std::get<1>(table_id_tuple));
    return false;
  }
}

bool BinderContext::GetColumnPosTuple(
    std::shared_ptr<BinderContext> current_context, const std::string& col_name,
    std::tuple<oid_t, oid_t, oid_t>& col_pos_tuple, std::string& table_alias,
    type::TypeId& value_type) {
  bool find_matched = false;
  while (current_context != nullptr && !find_matched) {
    for (auto entry : current_context->table_alias_map) {
      bool get_matched = GetColumnPosTuple(col_name, entry.second,
                                           col_pos_tuple, value_type);
      if (get_matched) {
        if (!find_matched) {
          // First match
          find_matched = true;
          table_alias = entry.first;
        } else {
          throw Exception("Ambiguous column name " + col_name);
        }
      }
    }
    current_context = current_context->GetUpperContext();
  }
  return find_matched;
}

bool BinderContext::GetTableObj(
    std::shared_ptr<BinderContext> current_context, std::string& alias,
    std::shared_ptr<catalog::TableCatalogObject>& table_obj) {
  while (current_context != nullptr) {
    auto iter = current_context->table_alias_map.find(alias);
    if (iter != current_context->table_alias_map.end()) {
      table_obj = iter->second;
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

}  // namespace binder
}  // namespace peloton
