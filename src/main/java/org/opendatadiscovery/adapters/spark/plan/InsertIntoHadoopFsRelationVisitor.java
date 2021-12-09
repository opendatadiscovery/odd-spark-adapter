package org.opendatadiscovery.adapters.spark.plan;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.opendatadiscovery.adapters.spark.mapper.DataEntityMapper;
import org.opendatadiscovery.client.model.DataEntity;

import java.util.Collections;
import java.util.List;

import static org.opendatadiscovery.adapters.spark.utils.Utils.namespaceUri;

public class InsertIntoHadoopFsRelationVisitor
    extends QueryPlanVisitor<InsertIntoHadoopFsRelationCommand, DataEntity> {

  @Override
  public List<DataEntity> apply(LogicalPlan logicalPlan) {
    InsertIntoHadoopFsRelationCommand command = (InsertIntoHadoopFsRelationCommand) logicalPlan;
    var outputPath = command.outputPath().toUri();
    var namespace = namespaceUri(outputPath);
    return Collections.singletonList(DataEntityMapper.map(outputPath));
  }
}