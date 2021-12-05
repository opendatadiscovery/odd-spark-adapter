package com.provectus.odd.adapters.spark.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.MysqlPath;
import org.opendatadiscovery.oddrn.model.PostgreSqlPath;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

@Slf4j
public class Utils {

    private static final String SLASH_DELIMITER_USER_PASSWORD_REGEX =
            "[A-Za-z0-9_%]+//?[A-Za-z0-9_%]*@";
    private static final String COLON_DELIMITER_USER_PASSWORD_REGEX =
            "([/|,])[A-Za-z0-9_%]+:?[A-Za-z0-9_%]*@";

    // strip the jdbc: prefix from the url. this leaves us with a url like
    // postgresql://<hostname>:<port>/<database_name>?params
    // we don't parse the URI here because different drivers use different connection
    // formats that aren't always amenable to how Java parses URIs. E.g., the oracle
    // driver format looks like oracle:<drivertype>:<user>/<password>@<database>
    // whereas postgres, mysql, and sqlserver use the scheme://hostname:port/db format.
    public static String sanitizeJdbcUrl(String jdbcUrl) {
        jdbcUrl = jdbcUrl.substring(5);
        return jdbcUrl
                .replaceAll(SLASH_DELIMITER_USER_PASSWORD_REGEX, "@")
                .replaceAll(COLON_DELIMITER_USER_PASSWORD_REGEX, "$1")
                .replaceAll("(?<=[?,;&:)=])\\(?(?i)(?:user|username|password)=[^;&,)]+(?:[;&;)]|$)", "");
    }

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
            var split = url.split("://");
            var tokens = split[1].split("/");
            switch (split[0]) {
                case "mysql" :
                    return new Generator().generate(MysqlPath.builder()
                            .host(tokens[0])
                            .database(tokens[1])
                            .table(tableName)
                            .build(), "table");
                case "postgresql" :
                    return new Generator().generate(PostgreSqlPath.builder()
                            .host(tokens[0])
                            .database(tokens[1])
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
    public static String fileGenerator(String namespace, String path, String fileName) {
        return "//" + namespace + path + (fileName == null ? "" : fileName);
    }
}