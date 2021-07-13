package com.provectus.odd.api;

import java.util.List;
import java.util.Map;

import static com.provectus.odd.api.DataEntity.JOB;
import static com.provectus.odd.api.DataEntity.JOB_RUN;

public class DataEntityBuilder {
    DataEntity underConstruction = new DataEntity();

    public DataEntityBuilder metadata(Map<String, String> metadata) {
        underConstruction.metadata = metadata;
        return this;
    }

    public DataEntityBuilder type(String aType) {
        underConstruction.type = aType;
        if (aType.equals(JOB)) {
            if (underConstruction.dataTransformerRun != null) {
                throw new IllegalArgumentException("Already set it to DataTranformerRun");
            }
            if (underConstruction.dataTransformer == null) {
                underConstruction.dataTransformer = new DataTransformer();
            }
        } else if (aType.equals(JOB_RUN)) {
            if (underConstruction.dataTransformer != null) {
                throw new IllegalArgumentException("Already set it to DataTranformer");
            }
            if (underConstruction.dataTransformerRun == null) {
                underConstruction.dataTransformerRun = new DataTransformerRun();
            }
        } else {
            throw new IllegalArgumentException("Unknown type: " + aType);
        }
        return this;
    }

    public DataEntity build() {
        return underConstruction;
    }

    public DataEntityBuilder source_code_url(String sourceCodeUrl) {
        ensureCorrectType(JOB);
        underConstruction.dataTransformer.sourceCodeUrl = sourceCodeUrl;
        return this;
    }

    public DataEntityBuilder name(String aName) {
        underConstruction.name = aName;
        return this;
    }

    public DataEntityBuilder sql(String aSql) {
        ensureCorrectType(JOB);
        underConstruction.dataTransformer.sql = aSql;
        return this;
    }

    public DataEntityBuilder oddrn(String aOddrn) {
        underConstruction.oddrn = aOddrn;
        return this;
    }

    public DataEntityBuilder description(String aDescription) {
        underConstruction.description = aDescription;
        return this;
    }

    public DataEntityBuilder owner(String aOwner) {
        underConstruction.owner = aOwner;
        return this;
    }

    public DataEntityBuilder inputs(List<String> aInputs) {
        ensureCorrectType(JOB);
        underConstruction.dataTransformer.inputs = aInputs;
        return this;
    }

    public DataEntityBuilder outputs(List<String> aOutputs) {
        ensureCorrectType(JOB);
        underConstruction.dataTransformer.outputs = aOutputs;
        return this;
    }

    private void ensureCorrectType(String expectedType) {
        if (underConstruction.type == null || !underConstruction.type.equals(expectedType)) {
            throw new IllegalArgumentException("Should be DataTransformer to do this");
        }
    }

    public DataEntityBuilder status(String aStatus) {
        ensureCorrectType(JOB_RUN);
        underConstruction.dataTransformerRun.status = aStatus;
        return this;
    }

    public DataEntityBuilder transformer_oddrn(String transformerOddrn) {
        ensureCorrectType(JOB_RUN);
        underConstruction.dataTransformerRun.transformerOddrn = transformerOddrn;
        return this;
    }

    public DataEntityBuilder start_time(String aStartTime) {
        ensureCorrectType(JOB_RUN);
        underConstruction.dataTransformerRun.startTime = aStartTime;
        return this;
    }

    public DataEntityBuilder end_time(String aEndTime) {
        ensureCorrectType(JOB_RUN);
        underConstruction.dataTransformerRun.endTime = aEndTime;
        return this;
    }

    public DataEntityBuilder status_reason(String aStatusReason) {
        ensureCorrectType(JOB_RUN);
        underConstruction.dataTransformerRun.statusReason = aStatusReason;
        return this;
    }
}
