package org.opendatadiscovery.adapters.spark;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.adapters.spark.visitor.QueryPlanVisitor;

import java.util.List;

public interface VisitorFactory {
    List<QueryPlanVisitor<? extends LogicalPlan>> getVisitors();
}
