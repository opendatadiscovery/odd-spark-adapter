package com.provectus.odd.adapters.spark.mapper;

import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.types.StructField;
import org.opendatadiscovery.client.model.DataSetField;
import org.opendatadiscovery.client.model.DataSetFieldType;
import org.opendatadiscovery.client.model.MetadataExtension;

import java.util.Collections;

import static com.provectus.odd.adapters.spark.utils.ScalaConversionUtils.asJava;

public class DataSetFieldMapper {

    public static DataSetFieldMapper INSTANCE = new DataSetFieldMapper();

    private DataSetFieldMapper() {
    }

    public DataSetField map(AttributeReference field) {
        return new DataSetField()
                .name(field.name())
                .type(new DataSetFieldType()
                        .type(getType(field.dataType().typeName()))
                        .logicalType(field.dataType().typeName())
                        .isNullable(field.nullable()))
                .metadata(Collections.singletonList(new MetadataExtension().metadata(asJava(field.metadata().map()))));
    }

    public DataSetField map(StructField field) {
        return new DataSetField()
                .name(field.name())
                .type(new DataSetFieldType()
                        .type(getType(field.dataType().typeName()))
                        .logicalType(field.dataType().typeName())
                        .isNullable(field.nullable()))
                .metadata(Collections.singletonList(new MetadataExtension().metadata(asJava(field.metadata().map()))));
    }

    private DataSetFieldType.TypeEnum getType(String value) {
        switch (value) {
            case "string":
                return DataSetFieldType.TypeEnum.STRING;
            case "integer":
                return DataSetFieldType.TypeEnum.INTEGER;
            case "double":
                return DataSetFieldType.TypeEnum.NUMBER;
        }
        return DataSetFieldType.TypeEnum.UNKNOWN;
    }
}
