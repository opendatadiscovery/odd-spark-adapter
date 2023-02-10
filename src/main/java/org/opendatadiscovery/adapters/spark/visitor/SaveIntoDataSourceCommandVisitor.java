package org.opendatadiscovery.adapters.spark.visitor;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.snowflake.spark.snowflake.DefaultSource;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.kafka010.KafkaSourceProvider;
import org.opendatadiscovery.adapters.spark.VisitorFactoryProvider;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;
import org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.KafkaPath;
import org.opendatadiscovery.oddrn.model.SnowflakePath;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractPartialFunction;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class SaveIntoDataSourceCommandVisitor extends QueryPlanVisitor<SaveIntoDataSourceCommand> {
    private static final String URL = "url";
    private static final String DBTABLE = "dbtable";

    private final SparkContext sparkContext;

    @Override
    public LogicalPlanDependencies apply(final LogicalPlan logicalPlan) {
        final List<QueryPlanVisitor<? extends LogicalPlan>> visitors = VisitorFactoryProvider
            .create(sparkContext)
            .getVisitors();

        try {
            final SaveIntoDataSourceCommand command = (SaveIntoDataSourceCommand) logicalPlan;

            return LogicalPlanDependencies.merge(Arrays.asList(
                extractOutput(command),
                extractDependencies(command, visitors)
            ));
        } catch (final Exception e) {
            log.error("Couldn't handle the logical plan: {}, reason: {}", logicalPlan.getClass(), e.getMessage());
            e.printStackTrace();
            return LogicalPlanDependencies.empty();
        }
    }

    @SneakyThrows
    private LogicalPlanDependencies extractOutput(final SaveIntoDataSourceCommand command) {
        if (command.dataSource().getClass().getName().contains("DeltaDataSource")) {
            if (command.options().contains("path")) {
                final String path = command.options().get("path").get();
                try {
                    return LogicalPlanDependencies.output(Utils.s3Generator(URI.create(path).getScheme(), path));
                } catch (final Exception e) {
                    log.error("Couldn't handle output for Delta Source: {}", command);
                    return LogicalPlanDependencies.empty();
                }
            }
        }

        if (KafkaRelationVisitor.hasKafkaClasses() && command.dataSource() instanceof KafkaSourceProvider) {
            final Map<String, String> options = JavaConverters.mapAsJavaMap(command.options());
            final String cluster = options.get("kafka.bootstrap.servers");
            final String topicName = options.getOrDefault("topic", "UNKNOWN");

            final KafkaPath kafkaPath = KafkaPath.builder()
                .cluster(cluster)
                .topic(topicName)
                .build();

            return LogicalPlanDependencies.output(Generator.getInstance().generate(kafkaPath));
        }

        if (SnowflakeRelationVisitor.hasSnowflakeClasses() && command.dataSource() instanceof DefaultSource) {
            final Map<String, String> options = JavaConverters.mapAsJavaMap(command.options());

            final SnowflakePath snowflakePath = SnowflakePath.builder()
                .account("account")
                .database(options.get("sfdatabase"))
                .schema(options.get("sfschema"))
                .table(options.getOrDefault("dbtable", "UNKNOWN"))
                .build();

            return LogicalPlanDependencies.output(Generator.getInstance().generate(snowflakePath));
        }

        final String url = command.options().get(URL).get();
        final String tableName = command.options().get(DBTABLE).get();

        return LogicalPlanDependencies.output(Utils.sqlGenerator(url, tableName));
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