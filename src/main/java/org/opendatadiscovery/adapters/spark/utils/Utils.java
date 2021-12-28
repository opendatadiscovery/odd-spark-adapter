package org.opendatadiscovery.adapters.spark.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.JdbcUrlParser;
import org.opendatadiscovery.oddrn.model.AwsS3Path;
import org.opendatadiscovery.oddrn.model.CustomS3Path;
import org.opendatadiscovery.oddrn.model.MysqlPath;
import org.opendatadiscovery.oddrn.model.PostgreSqlPath;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

@Slf4j
public class Utils {
    public static final String S3A_ENDPOINT = "fs.s3n.endpoint";
    public static final String S3N_ENDPOINT = "fs.s3a.endpoint";
    public static final String AMAZONAWS_COM = ".amazon.com";
    public static final String S3A = "s3a://";
    public static final String S3N = "s3n://";
    public static String CAMEL_TO_SNAKE_CASE =
            "[\\s\\-_]?((?<=.)[A-Z](?=[a-z\\s\\-_])|(?<=[^A-Z])[A-Z]|((?<=[\\s\\-_])[a-z\\d]))";

    public static String namespaceUri(URI outputPath) {
        return Optional.ofNullable(outputPath.getAuthority())
                .map(a -> String.format("%s://%s", outputPath.getScheme(), a))
                .orElse(outputPath.getScheme());
    }

    public static Path getDirectoryPath(Path p, Configuration hadoopConf) {
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

    public static String sqlGenerator(String url, String tableName) {
        try {
            var oddrnPath = new JdbcUrlParser().parse(url);
            switch (oddrnPath.prefix()) {
                case "//mysql" :
                    return new Generator().generate(((MysqlPath)oddrnPath)
                            .toBuilder()
                            .table(tableName)
                            .build(), "table");
                case "//postgresql" :
                    return new Generator().generate(((PostgreSqlPath)oddrnPath)
                            .toBuilder()
                            .schema("public")
                            .table(tableName)
                            .build(), "table");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return "!" + url + "/" + tableName;
    }

    //TODO use oddrn generator
    public static String fileGenerator(String namespace, String file) {
        return "//" + namespace + file.replace(namespace + ":/", "");
    }

    public static String s3Generator(String namespace, String key) {
        var bucket = "";
        var endpoint = "";
        if (namespace.contains(S3A)) {
            bucket = namespace.replace(S3A, "");
            endpoint = s3endpoint(S3A_ENDPOINT).orElse("");
        } else {
            bucket = namespace.replace(S3N, "");
            endpoint = s3endpoint(S3N_ENDPOINT).orElse("");
        }
        log.info("fs.s3n.endpoint: {}", endpoint);
        try {
            if (endpoint.isEmpty() || endpoint.contains(AMAZONAWS_COM)) {
                return new Generator().generate(AwsS3Path.builder()
                        .bucket(bucket)
                        .key(key.replace(namespace, "")).build(), "key");
            }
            return new Generator().generate(CustomS3Path.builder()
                    .endpoint(endpoint)
                    .bucket(bucket)
                    .key(key.replace(namespace, "")).build(), "key");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Optional<String> s3endpoint(String key) {
        return Optional
                //Think about to make SparkContext global or in dedicated context
                .ofNullable(SparkContext.getOrCreate().hadoopConfiguration()).map(h -> h.get(key));
    }
}