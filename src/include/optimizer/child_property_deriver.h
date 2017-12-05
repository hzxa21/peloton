//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// convert_query_to_op.h
//
// Identification: src/include/optimizer/child_property_generator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_visitor.h"

namespace peloton {

namespace optimizer {
class Memo;
}

namespace optimizer {

// Generate child property requirements for physical operators, return pairs of
// possible input output properties pairs.
// This is the new version of ChildPropertyGenerator, as the old version should
// be eventually deprecated
// TODO(boweic): Currently we only represent sort as property, later we may want
// to add group, data compression and data distribution(if we go distributed) as
// property
class ChildPropertyDeriver : public OperatorVisitor {
 public:
  std::vector<std::pair<PropertySet, std::vector<PropertySet>>> GetProperties(
      GroupExpression *gexpr, PropertySet requirements, Memo *memo);

  void Visit(const DummyScan *) override;
  void Visit(const PhysicalSeqScan *) override;
  void Visit(const PhysicalIndexScan *) override;
  void Visit(const QueryDerivedScan *op) override;
  void Visit(const PhysicalProject *) override;
  void Visit(const PhysicalOrderBy *) override;
  void Visit(const PhysicalLimit *) override;
  void Visit(const PhysicalFilter *) override;
  void Visit(const PhysicalInnerNLJoin *) override;
  void Visit(const PhysicalLeftNLJoin *) override;
  void Visit(const PhysicalRightNLJoin *) override;
  void Visit(const PhysicalOuterNLJoin *) override;
  void Visit(const PhysicalInnerHashJoin *) override;
  void Visit(const PhysicalLeftHashJoin *) override;
  void Visit(const PhysicalRightHashJoin *) override;
  void Visit(const PhysicalOuterHashJoin *) override;
  void Visit(const PhysicalInsert *) override;
  void Visit(const PhysicalInsertSelect *) override;
  void Visit(const PhysicalDelete *) override;
  void Visit(const PhysicalUpdate *) override;
  void Visit(const PhysicalHashGroupBy *) override;
  void Visit(const PhysicalSortGroupBy *) override;
  void Visit(const PhysicalDistinct *) override;
  void Visit(const PhysicalAggregate *) override;

 private:
  PropertySet requirements_;
  std::vector<std::pair<PropertySet, std::vector<PropertySet>>> output_;
  // We need the memo and gexpr because some property may depend on child's
  // schema
  Memo *memo_;
  GroupExpression *gexpr_;
};

}  // namespace optimizer
}  // namespace peloton