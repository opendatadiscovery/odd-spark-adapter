package org.opendatadiscovery.adapters.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.adapters.spark.plan.InsertIntoHadoopFsRelationVisitor;
import org.opendatadiscovery.adapters.spark.plan.LogicalRelationVisitor;
import org.opendatadiscovery.adapters.spark.plan.QueryPlanVisitor;
import org.opendatadiscovery.adapters.spark.plan.SaveIntoDataSourceCommandVisitor;
import org.opendatadiscovery.client.model.DataEntity;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class VisitorFactoryImpl implements VisitorFactory {

    @Override
    public List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getInputVisitors(final SparkContext sparkContext) {
        return Collections.singletonList(new LogicalRelationVisitor(sparkContext));
    }

    @Override
    public List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getOutputVisitors() {
        return Arrays.asList(
            new InsertIntoHadoopFsRelationVisitor(),
            new SaveIntoDataSourceCommandVisitor()
        );
    }
}
