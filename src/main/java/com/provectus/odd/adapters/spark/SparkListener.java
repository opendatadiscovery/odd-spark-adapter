package com.provectus.odd.adapters.spark;

import com.provectus.odd.api.DataEntity;
import com.provectus.odd.api.DataEntityBuilder;
import com.provectus.odd.api.EntitiesRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.*;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.SparkSession;
import scala.Option;

import java.io.IOException;
import java.net.URI;
import java.util.*;

@Slf4j
public class SparkListener extends org.apache.spark.scheduler.SparkListener {
    public static final String ENDPOINT_CONFIG_KEY = "opendatadiscovery.endpoint";
    public static final String SPARK_DATA_SOURCE_ODRNN = "//spark";
    private static JobRegistry registry = new JobRegistry();

    private OddClient client = null;

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        log.info("onApplicationStart({})", applicationStart);
        SparkEnv sparkEnv = SparkEnv$.MODULE$.get();
        SparkConf conf = sparkEnv.conf();
        String endpoint = conf.get(ENDPOINT_CONFIG_KEY, null);
        if (endpoint != null) {
            client = new OddClient(endpoint);
        } else {
            log.warn("No ODD emdpoint configured");
        }
    }

    public SparkListener() {
        super();
        System.out.println("Constructing SparkListener");
    }

    protected List<URI> findInputs(Set<RDD<?>> rdds) {
        List<URI> result = new ArrayList<>();
        for (RDD<?> rdd : rdds) {
            Path[] inputPaths = getInputPaths(rdd);
            if (inputPaths != null) {
                for (Path path : inputPaths) {
                    result.add(getDatasetUri(path.toUri()));
                }
            }
        }
        return result;
    }

    protected Path[] getInputPaths(RDD<?> rdd) {
        Path[] inputPaths = null;
        if (rdd instanceof HadoopRDD) {
            inputPaths =
                    org.apache.hadoop.mapred.FileInputFormat.getInputPaths(
                            ((HadoopRDD<?, ?>) rdd).getJobConf());
        } else if (rdd instanceof NewHadoopRDD) {
            try {
                inputPaths =
                        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.getInputPaths(
                                new Job(((NewHadoopRDD<?, ?>) rdd).getConf()));
            } catch (IOException e) {
                log.error("Could not get input paths", e);
            }
        } else {
            log.info("Unknown RDD type: {}", rdd);
        }
        return inputPaths;
    }

    // exposed for testing
    protected URI getDatasetUri(URI pathUri) {
        return pathUri;
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        log.info("onJobEnd(" + jobEnd + ")");
        JobInfo jobInfo = registry.getJobById(jobEnd.jobId());
        jobInfo.endedAt(jobEnd.time());
        DataEntityBuilder dataTransformerRunBuilder = jobInfo.getDataTransformerBuilder();
        JobResult jobResult = jobEnd.jobResult();
        String status;
        if (jobResult instanceof JobFailed) {
            status = "FAIL";
        } else {
            status = "SUCCESS";
        }
        dataTransformerRunBuilder.status(status);
        DataEntity dataEntity = dataTransformerRunBuilder.build();
        submit(dataEntity);
        registry.removeJob(jobEnd.jobId());
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        log.info("onJobStart({})", jobStart);

        DataEntityBuilder dataTransformerBuilder = DataEntity.builder().type(DataEntity.JOB);
        DataEntityBuilder dataTransformerRunBuilder = DataEntity.builder().type(DataEntity.JOB_RUN);

        JobInfo jobInfo = new JobInfo();
        jobInfo.setDataTransformerBuilder(dataTransformerBuilder);
        jobInfo.setDataTransformerRunBuilder(dataTransformerRunBuilder);
        registry.registerJob(jobStart.jobId(), jobInfo);

        Option<SparkSession> activeSession = SparkSession.getActiveSession();
        log.info("activeSession: " + activeSession);
        Option<SparkSession> defaultSession = SparkSession.getDefaultSession();
        log.info("defaultSession: " + defaultSession);
        log.info("defaultSession.getOrElse(): {}", defaultSession.getOrElse(()->(SparkSession)null));
        Option<SparkSession> sessionOpt = SparkSession.getActiveSession().getOrElse(SparkSession::getDefaultSession);
        String appName;

        if (Utils.asJavaOptional(sessionOpt).isPresent()) {
            SparkContext context = sessionOpt.get().sparkContext();
            appName = context.appName();

            SparkConf conf = context.getConf();
            log.info("conf: {}", Arrays.toString(conf.getAll()));
            log.info("appName: {}", appName);
            String sourceCodeUrl = conf.get("spark.app.initial.jar.urls", null);
            log.info("sourceCodeUrl: {}", sourceCodeUrl);
            dataTransformerBuilder.source_code_url(sourceCodeUrl);
            dataTransformerBuilder.name(appName);
            dataTransformerRunBuilder.name(appName);
        } else {
            log.info("no session");
            dataTransformerRunBuilder.name(null);
            dataTransformerBuilder.name(null);
            appName = null;
        }
        dataTransformerBuilder.sql(null); // TODO support spark sql

        Utils.asJavaOptional(
                sessionOpt
                        .map(Utils.toScalaFn(SparkSession::sparkContext))
                        .orElse(SparkContext$.MODULE$::getActive))
                .flatMap(ctx -> Utils.asJavaOptional(ctx.dagScheduler().jobIdToActiveJob().get(jobStart.jobId())))
                .ifPresent(
                        job -> {

                            String executionIdProp = job.properties().getProperty("spark.sql.execution.id");
                            System.out.println(Utils.activeJobToString(job));
                            System.out.println(job.properties());
                            if (executionIdProp != null) {
                                long executionId = Long.parseLong(executionIdProp);
                                System.out.println("Started job. executionId=" + executionId);
                            } else {
                                System.out.println("Started job. executionId unknown");
                            }
                            RDD<?> finalRDD = job.finalStage().rdd();
                            Set<RDD<?>> rdds = Utils.flattenRDDs(finalRDD);
                            List<URI> outputs = registry.findOutputs(rdds, this);
                            log.info("outputs = {}", outputs);
                            String oddrn = "//appName/" + appName + "/jobId/" + job.jobId();
//                            String startTime = String.valueOf(jobStart.time());
//                            log.info("startTime: {}", startTime);
                            dataTransformerRunBuilder.start_time(Utils.timestampToString(jobStart.time()));
                            dataTransformerBuilder.oddrn(oddrn);
                            dataTransformerRunBuilder.oddrn(oddrn);
                            dataTransformerRunBuilder.transformer_oddrn(oddrn);
                            dataTransformerBuilder.outputs(Utils.toStringList(outputs));
                            List<URI> inputs = findInputs(rdds);
                            log.info("inputs: {}", inputs);
                            dataTransformerBuilder.inputs(Utils.toStringList(inputs));
                            dataTransformerRunBuilder.status("OTHER");
                            dataTransformerRunBuilder.status_reason("STARTED");
                        });
        DataEntity dte = dataTransformerBuilder.build();
        System.out.println("DataTransformer entity:" + dte);
        submit(dte);

        DataEntity dtre = dataTransformerRunBuilder.build();
        System.out.println("DataTransformerRun:" + dtre);
        submit(dtre);
    }

    private void submit(DataEntity dt) {
        try {
            log.debug("Going to submit: {}", dt);
            if (client == null) {
                log.warn("No ODD client configured, can't report");
            } else {
                client.submit(new EntitiesRequest(SPARK_DATA_SOURCE_ODRNN, dt));
            }
        } catch (IOException e) {
            log.error("Error reporting to ODD", e);
        }
    }

    @SuppressWarnings("unused")
    public static void registerOutput(PairRDDFunctions<?, ?> pairRDDFunctions, Configuration conf) {
        System.out.printf("registerOutput(%s, %s)\n", pairRDDFunctions, conf);
        registry.registerOutput(pairRDDFunctions, conf);
    }

    public static void close() {
        log.info("SparkListener.close()");
    }
}
