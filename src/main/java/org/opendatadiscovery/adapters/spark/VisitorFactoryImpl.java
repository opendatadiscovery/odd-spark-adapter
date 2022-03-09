package org.opendatadiscovery.adapters.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.adapters.spark.plan.InsertIntoHadoopFsRelationVisitor;
import org.opendatadiscovery.adapters.spark.plan.LogicalRelationVisitor;
import org.opendatadiscovery.adapters.spark.plan.QueryPlanVisitor;
import org.opendatadiscovery.adapters.spark.plan.SaveIntoDataSourceCommandVisitor;
import org.opendatadiscovery.client.model.DataEntity;

@RequiredArgsConstructor
class VisitorFactoryImpl implements VisitorFactory {
    private final SparkContext sparkContext;

    @Override
    public List<QueryPlanVisitor<? extends LogicalPlan, DataEntity>> getInputVisitors(final SQLContext sqlContext) {
        if (sqlContext == null) {
            return Collections.singletonList(new LogicalRelationVisitor(sparkContext));
        }
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
