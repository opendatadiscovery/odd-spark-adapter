package com.provectus.odd.adapters.spark.mapper;

import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.StructType;
import org.opendatadiscovery.client.model.DataSet;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.provectus.odd.adapters.spark.utils.ScalaConversionUtils.fromSeq;

public class DataSetMapper {

    public static DataSetMapper INSTANCE = new DataSetMapper();

    private final DataSetFieldMapper dataSetFieldMapper = DataSetFieldMapper.INSTANCE;

    private DataSetMapper() {
    }

    public DataSet map(LogicalRelation logicalRelation) {
        return new DataSet()
                .fieldList(fromSeq(logicalRelation.output())
                        .stream()
                        .map(dataSetFieldMapper::map)
                        .collect(Collectors.toList()));
    }

    public DataSet map(StructType structType) {
        return new DataSet()
                .fieldList(Arrays.stream(structType.fields())
                        .map(dataSetFieldMapper::map)
                        .collect(Collectors.toList()));
    }
}
