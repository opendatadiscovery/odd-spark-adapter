package org.opendatadiscovery.adapters.spark.plan;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityType;
import scala.Option;

@Slf4j
public class SaveIntoDataSourceCommandVisitor extends QueryPlanVisitor<SaveIntoDataSourceCommand, DataEntity> {
    public static final String PATH = "path";
    public static final String URL = "url";
    public static final String DBTABLE = "dbtable";

    @Override
    public List<DataEntity> apply(final LogicalPlan logicalPlan) {
        final SaveIntoDataSourceCommand command = (SaveIntoDataSourceCommand) logicalPlan;

        final Option<String> pathOption = command.options().get(PATH);

        if (pathOption != null && pathOption.isDefined()) {
            return Collections.singletonList(new DataEntity()
                .type(DataEntityType.TABLE)
                // TODO: assuming it's an s3 path. Handle HDFS as well
                .oddrn(Utils.s3Generator(Utils.namespaceUri(URI.create(pathOption.get())), pathOption.get()))
            );
        }

        final String url = command.options().get(URL).get();
        final String tableName = command.options().get(DBTABLE).get();

        return Collections.singletonList(new DataEntity()
                .type(DataEntityType.TABLE)
                .oddrn(Utils.sqlGenerator(url, tableName))
        );
    }
}
