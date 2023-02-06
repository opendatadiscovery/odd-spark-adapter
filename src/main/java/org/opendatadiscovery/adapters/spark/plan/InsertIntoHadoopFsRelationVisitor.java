package org.opendatadiscovery.adapters.spark.plan;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;

import java.util.Collections;

public class InsertIntoHadoopFsRelationVisitor extends QueryPlanVisitor<InsertIntoHadoopFsRelationCommand> {
    @Override
    public LogicalPlanDependencies apply(final LogicalPlan logicalPlan) {
        final InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) logicalPlan;

        return LogicalPlanDependencies.outputs(Collections.singletonList(command.outputPath().toUri().toString()));
    }
}
