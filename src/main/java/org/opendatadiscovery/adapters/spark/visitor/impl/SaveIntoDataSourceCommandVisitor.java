package org.opendatadiscovery.adapters.spark.visitor.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.spark.snowflake.DefaultSource;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.kafka010.KafkaSourceProvider;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;
import org.opendatadiscovery.adapters.spark.utils.OddrnUtils;
import org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.adapters.spark.visitor.QueryPlanVisitor;
import org.opendatadiscovery.adapters.spark.visitor.VisitorFactoryProvider;
import org.opendatadiscovery.oddrn.model.KafkaPath;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractPartialFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

@Slf4j
@RequiredArgsConstructor
public class SaveIntoDataSourceCommandVisitor extends QueryPlanVisitor<SaveIntoDataSourceCommand> {
    private final SparkContext sparkContext;

    @Override
    public LogicalPlanDependencies apply(final LogicalPlan logicalPlan) {
        final List<QueryPlanVisitor<? extends LogicalPlan>> visitors = VisitorFactoryProvider
            .create(sparkContext)
            .getVisitors();

        final SaveIntoDataSourceCommand command = (SaveIntoDataSourceCommand) logicalPlan;

        LogicalPlanDependencies output;
        try {
            output = extractOutputPayload(command);
        } catch (final Exception e) {
            log.error("Couldn't extract output for SaveIntoDataSource command", e);
            output = LogicalPlanDependencies.empty();
        }

        LogicalPlanDependencies inputs;
        try {
            inputs = extractDependencies(command, visitors);
        } catch (final Exception e) {
            log.error("Couldn't extract input for SaveIntoDataSource command", e);
            inputs = LogicalPlanDependencies.empty();
        }

        return LogicalPlanDependencies.merge(Arrays.asList(inputs, output));
    }

    private LogicalPlanDependencies extractOutputPayload(final SaveIntoDataSourceCommand command) {
        if (command.dataSource().getClass().getName().contains("DeltaDataSource")) {
            if (command.options().contains("path")) {
                return OddrnUtils.resolveS3Oddrn(sparkContext.conf(), command.options().get("path").get())
                    .map(LogicalPlanDependencies::output)
                    .orElseGet(LogicalPlanDependencies::empty);
            }
        }

        if (KafkaRelationVisitor.hasKafkaClasses() && command.dataSource() instanceof KafkaSourceProvider) {
            final Map<String, String> options = JavaConverters.mapAsJavaMap(command.options());
            final String cluster = options.get("kafka.bootstrap.servers");
            final String topicName = options.getOrDefault("topic", "UNKNOWN");

            return LogicalPlanDependencies.output(
                KafkaPath.builder()
                    .cluster(cluster)
                    .topic(topicName)
                    .build()
            );
        }

        if (SnowflakeRelationVisitor.hasSnowflakeClasses() && command.dataSource() instanceof DefaultSource) {
            final Map<String, String> options = JavaConverters.mapAsJavaMap(command.options());

            final List<String> tableName = singletonList(options.getOrDefault("dbtable", "UNKNOWN"));

            return LogicalPlanDependencies.outputs(OddrnUtils.resolveSnowflakePath(tableName, options));
        }

        final String url = command.options().get("url").get();
        final String tableName = command.options().get("dbtable").get();

        return LogicalPlanDependencies.output(Utils.sqlOddrnPath(url, tableName));
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