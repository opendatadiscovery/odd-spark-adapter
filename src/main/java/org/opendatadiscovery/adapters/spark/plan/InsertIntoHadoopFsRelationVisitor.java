package org.opendatadiscovery.adapters.spark.plan;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;

import java.util.Collections;
import java.util.List;

public class InsertIntoHadoopFsRelationVisitor extends QueryPlanVisitor<InsertIntoHadoopFsRelationCommand, String> {
    @Override
    public List<String> apply(final LogicalPlan logicalPlan) {
        final InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) logicalPlan;

        return Collections.singletonList(command.outputPath().toUri().toString());
    }
}
