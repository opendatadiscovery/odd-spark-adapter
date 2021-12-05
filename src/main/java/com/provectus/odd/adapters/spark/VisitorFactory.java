package com.provectus.odd.adapters.spark;

import com.provectus.odd.adapters.spark.plan.QueryPlanVisitor;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.client.model.DataEntity;

import java.util.List;

interface VisitorFactory {

  List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getInputVisitors(SQLContext sqlContext);

  List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getOutputVisitors(SQLContext sqlContext);
}
