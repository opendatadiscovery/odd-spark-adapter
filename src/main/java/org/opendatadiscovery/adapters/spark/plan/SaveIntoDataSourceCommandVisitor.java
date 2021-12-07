package org.opendatadiscovery.adapters.spark.plan;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityType;

import java.util.Collections;
import java.util.List;

public class SaveIntoDataSourceCommandVisitor
    extends QueryPlanVisitor<SaveIntoDataSourceCommand, DataEntity> {
  public static final String URL = "url";
  public static final String DBTABLE = "dbtable";

  @Override
  public List<DataEntity> apply(LogicalPlan logicalPlan) {
    SaveIntoDataSourceCommand command = (SaveIntoDataSourceCommand) logicalPlan;
    var url = command.options().get(URL).get();
    var tableName = command.options().get(DBTABLE).get();
    return Collections.singletonList(new DataEntity()
            .type(DataEntityType.TABLE)
            .oddrn(Utils.sqlGenerator(url, tableName))
    );
  }
}
