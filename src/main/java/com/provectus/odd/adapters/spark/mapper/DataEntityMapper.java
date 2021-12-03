package com.provectus.odd.adapters.spark.mapper;

import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityType;
import scala.runtime.AbstractFunction0;

public class DataEntityMapper {

    public static DataEntityMapper INSTANCE = new DataEntityMapper();

    private final DataSetMapper dataSetMapper = DataSetMapper.INSTANCE;

    private DataEntityMapper() {
    }

    public DataEntity map(LogicalRelation logicalRelation) {
        return new DataEntity()
                .oddrn("/write")
                .type(DataEntityType.TABLE)
                .dataset(dataSetMapper.map(logicalRelation));
    }

    public DataEntity map(JDBCRelation relation) {
        // TODO- if a relation is composed of a complex sql query, we should attempt to
        // extract the
        // table names so that we can construct a true lineage
        String tableName =
                relation
                        .jdbcOptions()
                        .parameters()
                        .get(JDBCOptions.JDBC_TABLE_NAME())
                        .getOrElse(
                                new AbstractFunction0<String>() {
                                    @Override
                                    public String apply() {
                                        return "COMPLEX";
                                    }
                                });
        return new DataEntity()
                .oddrn("/read")
                .type(DataEntityType.TABLE)
                .name(tableName)
                .dataset(dataSetMapper.map(relation.schema()));
    }
}
