package org.opendatadiscovery.adapters.spark.mapper;

import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityList;
import org.opendatadiscovery.client.model.DataEntityType;
import org.opendatadiscovery.client.model.DataTransformer;
import org.opendatadiscovery.client.model.DataTransformerRun;
import org.opendatadiscovery.oddrn.Generator;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opendatadiscovery.adapters.spark.utils.Utils.S3;
import static org.opendatadiscovery.adapters.spark.utils.Utils.S3A;
import static org.opendatadiscovery.adapters.spark.utils.Utils.S3N;
import static org.opendatadiscovery.adapters.spark.utils.Utils.fileGenerator;
import static org.opendatadiscovery.adapters.spark.utils.Utils.s3Generator;

public class DataEntityMapper {
    public static final String SPARK_APP_NAME = "spark.app.name";
    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_APP_ID = "spark.app.id";
    public static final String SPARK_APP_START_TIME = "spark.app.startTime";
    private static final String JOBS = "/jobs";

    private DataEntityMapper() {
    }

    public static DataEntity map(final DataEntity dataEntity) {
        final DataTransformerRun transformerRun = dataEntity.getDataTransformerRun();
        return new DataEntity()
            .metadata(dataEntity.getMetadata())
            .createdAt(dataEntity.getCreatedAt())
            .name(transformerRun.getTransformerOddrn().split(JOBS)[1])
            .type(DataEntityType.JOB)
            .oddrn(transformerRun.getTransformerOddrn());
    }

    public static DataEntityList map(final DataEntity dataEntity, final List<DataEntity> inputs,
                                     final List<DataEntity> outputs) {
        return new DataEntityList()
            .dataSourceOddrn(dataEntity.getOddrn().split(JOBS)[0])
            .addItemsItem(DataEntityMapper
                .map(dataEntity)
                .dataTransformer(
                    new DataTransformer()
                        .sql(findSql(inputs).orElse(findSql(outputs).orElse(null)))
                        .inputs(inputs.stream().map(DataEntity::getOddrn).collect(Collectors.toList()))
                        .outputs(outputs.stream().map(DataEntity::getOddrn)
                            .collect(Collectors.toList()))
                )
            )
            .addItemsItem(dataEntity);
    }

    public static DataEntity map(final String sql, final String url, final String tableName) {
        return new DataEntity()
            .type(DataEntityType.TABLE)
            .dataTransformer(new DataTransformer().sql(sql))
            .oddrn(Generator.getInstance().generate(Utils.sqlOddrnPath(url, tableName)));
    }

    public static String map(final String namespace, final String file) {
        if (namespace.contains(S3A) || namespace.contains(S3N) || namespace.contains(S3)) {
            return s3Generator(namespace, file);
        }

        return fileGenerator(namespace, file);
    }

    private static Optional<String> findSql(final List<DataEntity> inputs) {
        return inputs.stream()
            .map(DataEntity::getDataTransformer)
            .filter(Objects::nonNull)
            .findFirst()
            .map(DataTransformer::getSql)
            .filter(s -> !s.isEmpty());
    }
}
