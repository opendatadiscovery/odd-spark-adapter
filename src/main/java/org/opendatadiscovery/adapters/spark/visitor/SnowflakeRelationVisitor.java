package org.opendatadiscovery.adapters.spark.visitor;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.snowflake.spark.snowflake.Parameters;
import net.snowflake.spark.snowflake.SnowflakeRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.oddrn.model.SnowflakePath;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SnowflakeRelationVisitor extends QueryPlanVisitor<LogicalRelation> {
    private static final String SNOWFLAKE_CLASS_NAME = "net.snowflake.spark.snowflake.SnowflakeRelation";
    private static final String SNOWFLAKE_PROVIDER_CLASS_NAME = "net.snowflake.spark.snowflake.DefaultSource";

    @Override
    public boolean isDefinedAt(final LogicalPlan logicalPlan) {
        try {
            Class<?> c = Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_CLASS_NAME);
            return logicalPlan instanceof LogicalRelation
                && c.isAssignableFrom(((LogicalRelation) logicalPlan).relation().getClass());
        } catch (final Exception e) {
            return false;
        }
    }

    @Override
    @SneakyThrows
    public LogicalPlanDependencies apply(final LogicalPlan x) {
        final SnowflakeRelation relation = (SnowflakeRelation) ((LogicalRelation) x).relation();
        final Parameters.MergedParameters snowflakeParams = relation.params();
        final Statement statement = CCJSqlParserUtil.parse(snowflakeParams.query().get());

        final List<OddrnPath> inputs = new TablesNamesFinder().getTableList(statement)
            .stream()
            .map(tableName -> SnowflakePath.builder()
                .database(snowflakeParams.sfDatabase())
                .schema(snowflakeParams.sfSchema())
                .table(tableName)
                .build())
            .collect(Collectors.toList());

        try {
            return LogicalPlanDependencies.inputs(inputs);
        } catch (final Exception e) {
            log.error("Couldn't get oddrn for SnowflakeRelation: {}", e.getMessage());
            return LogicalPlanDependencies.empty();
        }
    }

    public static boolean hasSnowflakeClasses() {
        try {
            SnowflakeRelationVisitor.class.getClassLoader().loadClass(SNOWFLAKE_PROVIDER_CLASS_NAME);
            return true;
        } catch (final Exception ignored) {
        }

        try {
            Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_PROVIDER_CLASS_NAME);
            return true;
        } catch (Exception ignored) {
        }

        return false;
    }
}
