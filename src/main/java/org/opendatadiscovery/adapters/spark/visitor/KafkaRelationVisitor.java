package org.opendatadiscovery.adapters.spark.visitor;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.kafka010.KafkaRelation;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.KafkaPath;
import scala.collection.JavaConverters;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class KafkaRelationVisitor extends QueryPlanVisitor<LogicalRelation> {
    @Override
    public boolean isDefinedAt(final LogicalPlan logicalPlan) {
        return logicalPlan instanceof LogicalRelation
            && ((LogicalRelation) logicalPlan).relation() instanceof KafkaRelation;
    }

    @Override
    @SneakyThrows
    public LogicalPlanDependencies apply(final LogicalPlan x) {
        final KafkaRelation relation = (KafkaRelation) ((LogicalRelation) x).relation();

        Map<String, String> options;
        try {
            Field sourceOptionsField = relation.getClass().getDeclaredField("sourceOptions");
            sourceOptionsField.setAccessible(true);
            options = JavaConverters.mapAsJavaMap(
                (scala.collection.immutable.Map<String, String>) sourceOptionsField.get(relation));
        } catch (final Exception e) {
            log.error("Can't extract kafka server options", e);
            return LogicalPlanDependencies.empty();
        }

        final String cluster = options.get("kafka.bootstrap.servers");
        final String topicNames = options.get("subscribe");

        final List<String> inputs = Arrays.stream(topicNames.split(","))
            .map(String::trim)
            .map(topicName -> {
                try {
                    return Generator.getInstance().generate(KafkaPath.builder()
                        .cluster(cluster)
                        .topic(topicName)
                        .build());
                } catch (final Exception e) {
                    log.error("Error", e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        return LogicalPlanDependencies.inputs(inputs);
    }

    public static boolean hasKafkaClasses() {
        try {
            KafkaRelationVisitor.class
                .getClassLoader()
                .loadClass("org.apache.spark.sql.kafka010.KafkaSourceProvider");
            return true;
        } catch (final Exception e) {
            return false;
        }
    }
}