package com.provectus.odd.adapters.spark.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.JdbcUrlParser;
import org.opendatadiscovery.oddrn.model.MysqlPath;
import org.opendatadiscovery.oddrn.model.PostgreSqlPath;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

@Slf4j
public class Utils {

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
    public static String fileGenerator(String namespace, String path, String fileName) {
        return "//" + namespace + path + (fileName == null ? "" : fileName);
    }
}