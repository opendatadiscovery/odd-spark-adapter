package org.opendatadiscovery.adapters.spark.mapper;

import org.apache.spark.scheduler.SparkListenerJobStart;

import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityList;
import org.opendatadiscovery.client.model.DataEntityType;
import org.opendatadiscovery.client.model.DataTransformer;
import org.opendatadiscovery.client.model.DataTransformerRun;
import org.opendatadiscovery.client.model.MetadataExtension;

import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.SparkPath;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;

public class DataEntityMapper {

    public static final String SPARK_APP_NAME = "spark.app.name";
    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_APP_ID = "spark.app.id";

    private DataEntityMapper() {}

    public static DataEntityList map(DataEntity dataEntity, List<DataEntity> inputs, List<DataEntity> outputs) {
        return new DataEntityList()
                .dataSourceOddrn(dataEntity.getOddrn().split("/jobs")[0])
                .addItemsItem(DataEntityMapper
                        .map(dataEntity)
                        .dataTransformer(
                                new DataTransformer()
                                        .sql(findSql(inputs).orElse(findSql(outputs).orElse(null)))
                                        .inputs(inputs.stream().map(DataEntity::getOddrn).collect(Collectors.toList()))
                                        .outputs(outputs.stream().map(DataEntity::getOddrn).collect(Collectors.toList()))
                        )
                )
                .addItemsItem(dataEntity);
    }

    private static Optional<String> findSql(List<DataEntity> inputs) {
        return inputs.stream()
                .map(DataEntity::getDataTransformer)
                .filter(Objects::nonNull)
                .findFirst()
                .map(DataTransformer::getSql)
                .filter(s -> !s.isEmpty());
    }

    public static DataEntity map(DataEntity dataEntity) {
        var transformerRun = dataEntity.getDataTransformerRun();
        return new DataEntity()
                .metadata(dataEntity.getMetadata())
                .createdAt(dataEntity.getCreatedAt())
                .name(transformerRun.getTransformerOddrn().split("jobs/")[1])
                .type(DataEntityType.JOB)
                .oddrn(transformerRun.getTransformerOddrn());
    }

    public static DataEntity map(String sql, String url, String tableName) {
        return new DataEntity()
                .type(DataEntityType.TABLE)
                .dataTransformer(new DataTransformer().sql(sql))
                .oddrn(Utils.sqlGenerator(url, tableName));
    }

    public static DataEntity map(SparkListenerJobStart jobStart) {
        var properties = jobStart.properties();
        var job = properties.getProperty(SPARK_APP_NAME);
        var host = Optional.ofNullable(properties.getProperty(SPARK_MASTER))
                .map(s -> s.split("://"))
                .map(s -> s.length > 1 ? s[1] : s[0])
                .map(s -> s.split(":")[0])
                .orElse(null);
        var run = properties.getProperty(SPARK_APP_ID);
        try {
            Map props = properties;
            var startTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(jobStart.time()), UTC);
            return new DataEntity()
                    .name(run)
                    .createdAt(startTime)
                    .metadata(singletonList(new MetadataExtension()
                            .metadata(new HashMap<>((Map<String, Object>) props))))
                    .type(DataEntityType.JOB_RUN)
                    .oddrn(new Generator().generate(SparkPath.builder()
                            .host(host)
                            .job(job)
                            .run(run)
                            .build(), "run")
                    )
                    .dataTransformerRun(new DataTransformerRun()
                            .startTime(startTime)
                            .transformerOddrn(new Generator().generate(SparkPath.builder()
                                            .host(host)
                                            .job(job)
                                            .build(), "job"))
                    );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
