package org.opendatadiscovery.adapters.spark.plan;

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
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.SnowflakePath;

import java.util.List;
import java.util.Objects;
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
            log.info(e.getMessage());
            return false;
        }
    }

    @Override
    @SneakyThrows
    public LogicalPlanDependencies apply(final LogicalPlan x) {
        final SnowflakeRelation relation = (SnowflakeRelation) ((LogicalRelation) x).relation();
        final Parameters.MergedParameters snowflakeParams = relation.params();

        final Statement statement = CCJSqlParserUtil.parse(snowflakeParams.query().get());

        final Generator generator = new Generator();

        final List<String> inputs = new TablesNamesFinder().getTableList(statement)
            .stream()
            .map(tableName -> SnowflakePath.builder()
                .database(snowflakeParams.sfDatabase())
                .warehouse(snowflakeParams.sfWarehouse().getOrElse(() -> "UNKNOWN"))
                .schema(snowflakeParams.sfSchema())
                .table(tableName)
                .build())
            .map(path -> {
                try {
                    return generator.generate(path, "table");
                } catch (final Exception e) {
                    log.error("Couldn't construct oddrn for input snowflake table: {}", e.getMessage());
                    e.printStackTrace();
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        try {
            return LogicalPlanDependencies.inputs(inputs);
        } catch (final Exception e) {
            log.error("Couldn't get oddrn for SnowflakeRelation: {}", e.getMessage());
            return LogicalPlanDependencies.empty();
        }
    }

    public static boolean hasSnowflakeClasses() {
        /**
         * Checking the Snowflake class with both
         * SnowflakeRelationVisitor.class.getClassLoader.loadClass and
         * Thread.currentThread().getContextClassLoader().loadClass. The first checks if the class is
         * present on the classpath, and the second one is a catchall which captures if the class has
         * been installed. This is relevant for Azure Databricks where jars can be installed and
         * accessible to the user, even if they are not present on the classpath.
         */
        try {
            SnowflakeRelationVisitor.class.getClassLoader().loadClass(SNOWFLAKE_PROVIDER_CLASS_NAME);
            return true;
        } catch (Exception e) {
            // swallow - we don't care
        }
        try {
            Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_PROVIDER_CLASS_NAME);
            return true;
        } catch (Exception e) {
            // swallow - we don't care
        }
        return false;
    }
}
