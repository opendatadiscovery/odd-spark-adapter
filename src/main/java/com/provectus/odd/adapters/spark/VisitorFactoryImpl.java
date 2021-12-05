package com.provectus.odd.adapters.spark;

import com.provectus.odd.adapters.spark.plan.InsertIntoHadoopFsRelationVisitor;
import com.provectus.odd.adapters.spark.plan.LogicalRelationVisitor;
import com.provectus.odd.adapters.spark.plan.QueryPlanVisitor;
import com.provectus.odd.adapters.spark.plan.SaveIntoDataSourceCommandVisitor;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.client.model.DataEntity;

import java.util.List;

class VisitorFactoryImpl implements VisitorFactory {

  @Override
  public List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getInputVisitors(SQLContext sqlContext) {
    return List.of(new LogicalRelationVisitor(sqlContext.sparkContext()));
  }

  @Override
  public List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getOutputVisitors(SQLContext sqlContext) {
    return List.of(
            new InsertIntoHadoopFsRelationVisitor(),
            new SaveIntoDataSourceCommandVisitor()
    );
  }
}
