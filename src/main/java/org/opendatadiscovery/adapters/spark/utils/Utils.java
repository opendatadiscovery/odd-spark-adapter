package org.opendatadiscovery.adapters.spark.utils;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.JdbcUrlParser;
import org.opendatadiscovery.oddrn.annotation.PathField;
import org.opendatadiscovery.oddrn.model.CustomS3Path;
import org.opendatadiscovery.oddrn.model.HdfsPath;
import org.opendatadiscovery.oddrn.model.MysqlPath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.oddrn.model.PostgreSqlPath;

@Slf4j
public class Utils {
    public static final String S3A_ENDPOINT = "fs.s3a.endpoint";
    public static final String S3N_ENDPOINT = "fs.s3n.endpoint";
    public static final String AMAZONAWS_COM = ".amazonaws.com";
    public static final String S3 = "s3://";
    public static final String S3A = "s3a://";
    public static final String S3N = "s3n://";
    public static final String HDFS = "hdfs://";
    public static String CAMEL_TO_SNAKE_CASE =
            "[\\s\\-_]?((?<=.)[A-Z](?=[a-z\\s\\-_])|(?<=[^A-Z])[A-Z]|((?<=[\\s\\-_])[a-z\\d]))";

    public static String namespaceUri(final URI outputPath) {
        return Optional.ofNullable(outputPath.getAuthority())
                .map(a -> String.format("%s://%s", outputPath.getScheme(), a))
                .orElse(outputPath.getScheme());
    }

    public static Path getDirectoryPath(final Path p, final Configuration hadoopConf) {
        try {
            if (p.getFileSystem(hadoopConf).getFileStatus(p).isFile()) {
                return p.getParent();
            } else {
                return p;
            }
        } catch (IOException e) {
            log.warn("Unable to get file system for path ", e);
            return p;
        }
    }

    public static String sqlGenerator(final String url, final String tableName) {
        try {
            final OddrnPath oddrnPath = new JdbcUrlParser().parse(url);
            switch (oddrnPath.prefix()) {
                case "//mysql" :
                    return new Generator().generate(((MysqlPath) oddrnPath)
                            .toBuilder()
                            .table(tableName)
                            .build(), "table");
                case "//postgresql" :
                    return new Generator().generate(((PostgreSqlPath) oddrnPath)
                            .toBuilder()
                            .schema("public")
                            .table(tableName)
                            .build(), "table");
                default:
                    return "!" + url + "/" + tableName;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String fileGenerator(final String namespace, final String file) {
        if (namespace.contains(HDFS)) {
            try {
                return new Generator().generate(HdfsPath.builder()
                        .site(namespace.replace(HDFS, ""))
                        .path(file).build(), "path");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return "//" + namespace + file.replace(namespace + ":/", "");
    }


    public static String s3Generator(final String namespace, final String path) {
        String bucket;
        String endpoint = "";
        if (namespace.contains(S3A)) {
            bucket = namespace.replace(S3A, "");
            endpoint = s3endpoint(S3A_ENDPOINT).orElse("");
        } else if (namespace.contains(S3N)) {
            bucket = namespace.replace(S3N, "");
            endpoint = s3endpoint(S3N_ENDPOINT).orElse("");
        } else {
            bucket = namespace.replace(S3, "");
        }
        String key = path.replace(namespace, "");
        key = key.startsWith("/") ? key.substring(1) : key;
        try {
            if (endpoint.isEmpty() || endpoint.contains(AMAZONAWS_COM)) {
                final AwsS3Path.AwsS3PathBuilder builder = AwsS3Path.builder()
                        .bucket(bucket);
                if (key.isEmpty()) {
                    return new Generator().generate(builder.build(), "bucket");
                }
                return new Generator().generate(builder.key(key).build(), "key");
            }
            final CustomS3Path.CustomS3PathBuilder builder = CustomS3Path.builder()
                    .endpoint(endpoint)
                    .bucket(bucket);
            if (key.isEmpty()) {
                return new Generator().generate(builder.build(), "bucket");
            }
            return new Generator().generate(builder.key(key).build(), "key");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Optional<String> s3endpoint(final String key) {
        return Optional
                .ofNullable(SparkContext.getOrCreate().hadoopConfiguration()).map(h -> h.get(key));
    }

    public static String getProperty(final Properties properties, final String key) {
        final String[] tokens = properties.toString().split("--" + key + " ");
        if (tokens.length > 1) {
            return tokens[1].split(" --")[0];
        }
        return null;
    }
}