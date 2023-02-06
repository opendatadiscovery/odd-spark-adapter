package org.opendatadiscovery.adapters.spark.plan;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.spark.snowflake.DefaultSource;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.opendatadiscovery.adapters.spark.VisitorFactoryImpl;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;
import org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.SnowflakePath;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractPartialFunction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
// TODO: InsertIntoDataSourceCommand
// TODO: what if I write a query to run inside of snowflake? This won't be a SaveIntoDataSourceCommand
public class SaveIntoDataSourceCommandVisitor extends QueryPlanVisitor<SaveIntoDataSourceCommand> {
    public static final String URL = "url";
    public static final String DBTABLE = "dbtable";

    @Override
    public LogicalPlanDependencies apply(final LogicalPlan logicalPlan) {
        final SparkContext sparkContext = SparkContext$.MODULE$.getActive().get();

        final List<QueryPlanVisitor<? extends LogicalPlan>> inputVisitors
            = new VisitorFactoryImpl().getVisitors(sparkContext);

        try {
            final SaveIntoDataSourceCommand command = (SaveIntoDataSourceCommand) logicalPlan;

            return LogicalPlanDependencies.merge(Arrays.asList(
                extractOutput(command),
                extractDependencies(command, inputVisitors)
            ));
        } catch (final Exception e) {
            log.error("Couldn't handle the logical plan: {}, reason: {}", logicalPlan.getClass(), e.getMessage());
            e.printStackTrace();
            return LogicalPlanDependencies.empty();
        }
    }

    @SneakyThrows
    private LogicalPlanDependencies extractOutput(final SaveIntoDataSourceCommand command) {
        if (SnowflakeRelationVisitor.hasSnowflakeClasses() && command.dataSource() instanceof DefaultSource) {
            final Map<String, String> options = JavaConverters.mapAsJavaMap(command.options());

            final SnowflakePath snowflakePath = SnowflakePath.builder()
                .database(options.get("sfdatabase"))
                .schema(options.get("sfschema"))
                .table(options.getOrDefault("dbtable", "UNKNOWN"))
                .warehouse("UNKNOWN")
                .build();

            return LogicalPlanDependencies
                .outputs(Collections.singletonList(new Generator().generate(snowflakePath, "table")));
        }

        final String url = command.options().get(URL).get();
        final String tableName = command.options().get(DBTABLE).get();

        return LogicalPlanDependencies.outputs(Collections.singletonList(Utils.sqlGenerator(url, tableName)));
    }

    private LogicalPlanDependencies extractDependencies(
        final SaveIntoDataSourceCommand command,
        final List<QueryPlanVisitor<? extends LogicalPlan>> visitors
    ) {
        final Seq<LogicalPlanDependencies> seq = command.query()
            .collect(new AbstractPartialFunction<LogicalPlan, LogicalPlanDependencies>() {
                @Override
                public boolean isDefinedAt(final LogicalPlan x) {
                    return true;
                }

                @Override
                public LogicalPlanDependencies apply(final LogicalPlan lp) {
                    final List<LogicalPlanDependencies> dependencies = visitors.stream()
                        .filter(v -> v.isDefinedAt(lp))
                        .map(iv -> iv.apply(lp))
                        .collect(Collectors.toList());

                    return LogicalPlanDependencies.merge(dependencies);
                }
            });

        return LogicalPlanDependencies.merge(ScalaConversionUtils.fromSeq(seq));
    }
}