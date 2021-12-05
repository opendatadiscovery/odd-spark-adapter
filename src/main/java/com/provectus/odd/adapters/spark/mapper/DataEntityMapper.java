package com.provectus.odd.adapters.spark.mapper;

import org.apache.spark.scheduler.SparkListenerJobStart;

import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityList;
import org.opendatadiscovery.client.model.DataEntityType;
import org.opendatadiscovery.client.model.DataTransformer;
import org.opendatadiscovery.client.model.DataTransformerRun;

import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.SparkPath;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;

public class DataEntityMapper {

    public static final String SPARK_APP_NAME = "spark.app.name";
    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_APP_ID = "spark.app.id";

    private DataEntityMapper() {}

    public static DataEntityList map(DataEntity dataEntity, List<DataEntity> inputs, List<DataEntity> outputs) {
        return new DataEntityList()
                .dataSourceOddrn(dataEntity.getOddrn())
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
        return new DataEntity()
                .type(DataEntityType.JOB)
                .oddrn(dataEntity.getDataTransformerRun().getTransformerOddrn());
    }

    public static DataEntity map(SparkListenerJobStart jobStart) {
        var properties = jobStart.properties();
        var job = properties.getProperty(SPARK_APP_NAME);
        var host = properties.getProperty(SPARK_MASTER).split("://")[1];
        var run = properties.getProperty(SPARK_APP_ID);
        try {
            return new DataEntity()

                    .type(DataEntityType.JOB_RUN)
                    .oddrn(new Generator().generate(SparkPath.builder()
                            .host(host)
                            .job(job)
                            .run(run)
                            .build(), "run")
                    )
                    .dataTransformerRun(new DataTransformerRun()
                            .startTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(jobStart.time()), UTC))
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
