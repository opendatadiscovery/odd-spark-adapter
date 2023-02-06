package org.opendatadiscovery.adapters.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.adapters.spark.plan.InsertIntoHadoopFsRelationVisitor;
import org.opendatadiscovery.adapters.spark.plan.LogicalRelationVisitor;
import org.opendatadiscovery.adapters.spark.plan.QueryPlanVisitor;
import org.opendatadiscovery.adapters.spark.plan.SaveIntoDataSourceCommandVisitor;
import org.opendatadiscovery.adapters.spark.plan.SnowflakeRelationVisitor;

import java.util.Arrays;
import java.util.List;

public class VisitorFactoryImpl implements VisitorFactory {
    @Override
    public List<QueryPlanVisitor<? extends LogicalPlan>> getVisitors(final SparkContext sparkContext) {
        return Arrays.asList(
            new LogicalRelationVisitor(sparkContext),
            new InsertIntoHadoopFsRelationVisitor(),
            new SaveIntoDataSourceCommandVisitor(),
            new SnowflakeRelationVisitor()
        );
    }
}
