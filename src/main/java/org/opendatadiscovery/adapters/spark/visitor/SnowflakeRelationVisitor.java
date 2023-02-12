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
import org.opendatadiscovery.adapters.spark.utils.OddrnUtils;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.oddrn.model.SnowflakePath;
import scala.collection.JavaConverters;

import java.util.List;

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

        final List<? extends OddrnPath> snowflakePaths = OddrnUtils.resolveSnowflakePath(
            new TablesNamesFinder().getTableList(statement),
            JavaConverters.mapAsJavaMap(relation.params().parameters())
        );

        return LogicalPlanDependencies.inputs(snowflakePaths);
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
