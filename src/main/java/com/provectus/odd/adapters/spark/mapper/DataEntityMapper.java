package com.provectus.odd.adapters.spark.mapper;

import org.apache.spark.scheduler.SparkListenerJobStart;

import org.opendatadiscovery.client.model.DataEntity;

import org.opendatadiscovery.client.model.DataEntityType;
import org.opendatadiscovery.client.model.DataTransformerRun;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.SparkPath;

import java.time.Instant;
import java.time.OffsetDateTime;

import static java.time.ZoneOffset.UTC;

public class DataEntityMapper {

    public DataEntity map(SparkListenerJobStart jobStart) {
        var properties = jobStart.properties();
        var job = properties.getProperty("spark.app.name");
        var host = properties.getProperty("spark.master").split("://")[1];
        var run = properties.getProperty("spark.app.id");
        try {
            return new DataEntity()
                    .type(DataEntityType.JOB)
                    .oddrn(new Generator().generate(SparkPath.builder()
                            .host(host)
                            .job(job)
                            .build(), "job"))
                    .dataTransformerRun(new DataTransformerRun()
                            .startTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(jobStart.time()), UTC))
                            .transformerOddrn(new Generator().generate(SparkPath.builder()
                                            .host(host)
                                            .job(job)
                                            .run(run)
                                            .build(), "run")));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
