package org.opendatadiscovery.adapters.spark.visitor;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.List;

public interface VisitorFactory {
    List<QueryPlanVisitor<? extends LogicalPlan>> getVisitors();
}
