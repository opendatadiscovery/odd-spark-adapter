package org.opendatadiscovery.adapters.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.adapters.spark.plan.QueryPlanVisitor;
import org.opendatadiscovery.client.model.DataEntity;

import java.util.List;

interface VisitorFactory {

    List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getInputVisitors(SparkContext sparkContext);

    List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getOutputVisitors();
}
