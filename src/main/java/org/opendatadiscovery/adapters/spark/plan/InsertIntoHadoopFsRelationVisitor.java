package org.opendatadiscovery.adapters.spark.plan;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.opendatadiscovery.adapters.spark.mapper.DataEntityMapper;
import org.opendatadiscovery.client.model.DataEntity;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class InsertIntoHadoopFsRelationVisitor
        extends QueryPlanVisitor<InsertIntoHadoopFsRelationCommand, DataEntity> {

  @Override
  public List<DataEntity> apply(LogicalPlan logicalPlan) {
    InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) logicalPlan;
    return Collections.singletonList(
            DataEntityMapper.map(
                    Optional.ofNullable(command)
                            .map(InsertIntoHadoopFsRelationCommand::outputPath)
                            .map(Path::toUri)
                            .orElseThrow()
            )
    );
  }
}
