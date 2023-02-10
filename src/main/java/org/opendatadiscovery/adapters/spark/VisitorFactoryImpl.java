package org.opendatadiscovery.adapters.spark;

import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.adapters.spark.visitor.InsertIntoHadoopFsRelationVisitor;
import org.opendatadiscovery.adapters.spark.visitor.KafkaRelationVisitor;
import org.opendatadiscovery.adapters.spark.visitor.LogicalRelationVisitor;
import org.opendatadiscovery.adapters.spark.visitor.QueryPlanVisitor;
import org.opendatadiscovery.adapters.spark.visitor.SaveIntoDataSourceCommandVisitor;
import org.opendatadiscovery.adapters.spark.visitor.SnowflakeRelationVisitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VisitorFactoryImpl implements VisitorFactory {
    @Getter
    private final List<QueryPlanVisitor<? extends LogicalPlan>> visitors;

    public VisitorFactoryImpl(final SparkContext sparkContext) {
        final List<QueryPlanVisitor<? extends LogicalPlan>> visitors = new ArrayList<>(Arrays.asList(
            new LogicalRelationVisitor(sparkContext),
            new InsertIntoHadoopFsRelationVisitor(),
            new SaveIntoDataSourceCommandVisitor(sparkContext)
        ));

        if (SnowflakeRelationVisitor.hasSnowflakeClasses()) {
            visitors.add(new SnowflakeRelationVisitor());
        }

        if (KafkaRelationVisitor.hasKafkaClasses()) {
            visitors.add(new KafkaRelationVisitor());
        }

        this.visitors = visitors;
    }
}
