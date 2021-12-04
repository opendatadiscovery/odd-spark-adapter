package com.provectus.odd.adapters.spark.mapper;

import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.opendatadiscovery.client.model.DataTransformer;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.MysqlPath;
import org.opendatadiscovery.oddrn.model.PostgreSqlPath;
import scala.runtime.AbstractFunction0;

import java.util.Collections;

import static com.provectus.odd.adapters.spark.utils.Utils.sanitizeJdbcUrl;

public class DataTransformerMapper {

    public static final String URL = "url";
    public static final String DBTABLE = "dbtable";

    public DataTransformer map(JDBCRelation relation) {
        // TODO- if a relation is composed of a complex sql query, we should attempt to
        // extract the
        // table names so that we can construct a true lineage
        String tableName =
                relation
                        .jdbcOptions()
                        .parameters()
                        .get(JDBCOptions.JDBC_TABLE_NAME())
                        .getOrElse(
                                new AbstractFunction0<>() {
                                    @Override
                                    public String apply() {
                                        return "COMPLEX";
                                    }
                                });
        return new DataTransformer()
                .inputs(Collections.singletonList(generate(sanitizeJdbcUrl(relation.jdbcOptions().url()), tableName)));
    }

    public DataTransformer map(SaveIntoDataSourceCommand command) {
        var url = sanitizeJdbcUrl(command.options().get(URL).get());
        var tableName = command.options().get(DBTABLE).get();
        return new DataTransformer()
                .outputs(Collections.singletonList(generate(url, tableName)));
    }

    private String generate(String url, String tableName) {
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
}
