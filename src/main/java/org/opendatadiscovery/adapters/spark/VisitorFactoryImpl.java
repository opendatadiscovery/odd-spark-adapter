package org.opendatadiscovery.adapters.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.adapters.spark.plan.InsertIntoHadoopFsRelationVisitor;
import org.opendatadiscovery.adapters.spark.plan.LogicalRelationVisitor;
import org.opendatadiscovery.adapters.spark.plan.QueryPlanVisitor;
import org.opendatadiscovery.adapters.spark.plan.SaveIntoDataSourceCommandVisitor;
import org.opendatadiscovery.client.model.DataEntity;

class VisitorFactoryImpl implements VisitorFactory {

    @Override
    public List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getInputVisitors(final SQLContext sqlContext) {
        return Collections.singletonList(new LogicalRelationVisitor(sqlContext.sparkContext()));
    }

    @Override
    public List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getOutputVisitors(final SQLContext sqlContext) {
        return Arrays.asList(
                new InsertIntoHadoopFsRelationVisitor(),
                new SaveIntoDataSourceCommandVisitor()
        );
    }
}
