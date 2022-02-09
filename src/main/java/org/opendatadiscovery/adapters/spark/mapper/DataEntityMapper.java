package org.opendatadiscovery.adapters.spark.mapper;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.spark.SparkContext;
import org.opendatadiscovery.adapters.spark.utils.Utils;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityList;
import org.opendatadiscovery.client.model.DataEntityType;
import org.opendatadiscovery.client.model.DataTransformer;
import org.opendatadiscovery.client.model.DataTransformerRun;
import org.opendatadiscovery.client.model.MetadataExtension;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.SparkPath;

import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;
import static org.opendatadiscovery.adapters.spark.utils.Utils.S3;
import static org.opendatadiscovery.adapters.spark.utils.Utils.S3A;
import static org.opendatadiscovery.adapters.spark.utils.Utils.S3N;
import static org.opendatadiscovery.adapters.spark.utils.Utils.fileGenerator;
import static org.opendatadiscovery.adapters.spark.utils.Utils.namespaceUri;
import static org.opendatadiscovery.adapters.spark.utils.Utils.s3Generator;

public class DataEntityMapper {

    public static final String SPARK_APP_NAME = "spark.app.name";
    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_APP_ID = "spark.app.id";
    public static final String SPARK_APP_START_TIME = "spark.app.startTime";
    private static final String JOBS = "/jobs";

    private DataEntityMapper() {}

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
                .oddrn(Utils.sqlGenerator(url, tableName));
    }

    public static DataEntity map(final SparkContext context) {
        final Properties properties = new Properties();
        properties.putAll(Arrays.stream(context.conf().toDebugString().split("\n"))
                .map(l -> l.split("="))
                .collect(Collectors.toMap(e -> e[0], e -> (Object) (e.length > 1 ? e[1] : ""))));
        return DataEntityMapper.map(properties);
    }

    public static DataEntity map(final Properties properties) {
        final String job = properties.getProperty(SPARK_APP_NAME);
        final String host = Optional.ofNullable(properties.getProperty(SPARK_MASTER))
                .map(s -> s.split("://"))
                .map(s -> s.length > 1 ? s[1] : s[0])
                .map(s -> s.split(":")[0])
                .orElse(null);
        final String run = properties.getProperty(SPARK_APP_ID);
        try {
            final Map props = properties;
            final OffsetDateTime startTime = OffsetDateTime
                    .ofInstant(Instant.ofEpochMilli(Long.parseLong(properties.getProperty(SPARK_APP_START_TIME))), UTC);
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

    public static DataEntity map(final String namespace, final String file) {
        if (namespace.contains(S3A) || namespace.contains(S3N) || namespace.contains(S3)) {
            return new DataEntity()
                    .type(DataEntityType.FILE)
                    .oddrn(s3Generator(namespace, file));
        }
        return new DataEntity()
                .type(DataEntityType.FILE)
                .oddrn(fileGenerator(namespace, file));
    }

    public static DataEntity map(final URI uri) {
        if (uri == null) {
            return null;
        }
        return DataEntityMapper.map(namespaceUri(uri), uri.getPath());
    }

    public static List<DataEntity> map(final List<URI> uris) {
        return uris.stream().map(DataEntityMapper::map).filter(Objects::nonNull).collect(Collectors.toList());
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
