package org.opendatadiscovery.adapters.spark.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.opendatadiscovery.oddrn.model.AwsS3Path;
import org.opendatadiscovery.oddrn.model.CustomS3Path;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.oddrn.model.SnowflakePath;
import scala.Option;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils.asJavaOptional;

@UtilityClass
@Slf4j
public class OddrnUtils {
    public static List<SnowflakePath> resolveSnowflakePath(final List<String> tableNames,
                                                           final Map<String, String> options) {
        final String account = options.get("sfurl").split("\\.")[0];
        final String database = options.get("sfdatabase");
        final String schema = options.get("sfschema");

        return tableNames.stream()
            .map(tableName -> SnowflakePath.builder()
                .account(account)
                .database(database)
                .schema(schema)
                .table(tableName)
                .build())
            .collect(Collectors.toList());
    }

    public static Optional<OddrnPath> resolveS3Oddrn(final SparkConf sparkConf, final String path) {
        final Option<String> endpoint;

        if (path.startsWith("s3a://")) {
            endpoint = sparkConf.getOption("spark.hadoop.fs.s3a.endpoint");
        } else if (path.startsWith("s3n://")) {
            endpoint = sparkConf.getOption("spark.hadoop.fs.s3n.endpoint");
        } else {
            log.warn("S3A and S3N are only schemes that are supported");
            return Optional.empty();
        }

        return asJavaOptional(endpoint.map(e -> getS3Oddrn(e, path)).orElse(() -> Option.apply(getS3Oddrn(path))));
    }

    private static OddrnPath getS3Oddrn(final String path) {
        return getS3Oddrn(null, path);
    }

    private static OddrnPath getS3Oddrn(final String endpoint, final String path) {
        final Pair<String, String> pathPayload = extractS3PayloadFromPath(path);

        if (endpoint == null) {
            return AwsS3Path.builder()
                .bucket(pathPayload.getKey())
                .key(pathPayload.getValue())
                .build();
        }

        return CustomS3Path.builder()
            .endpoint(endpoint)
            .bucket(pathPayload.getKey())
            .key(pathPayload.getValue())
            .build();
    }

    private static Pair<String, String> extractS3PayloadFromPath(final String path) {
        final String[] splitStructure = path.split("//");
        if (splitStructure.length != 2) {
            throw new IllegalArgumentException(String.format("Couldn't parse S3 path: %s", path));
        }

        final String[] splitPath = splitStructure[1].split("/");

        return Pair.of(splitPath[0], String.join("/", Arrays.copyOfRange(splitPath, 1, splitPath.length)));
    }
}
