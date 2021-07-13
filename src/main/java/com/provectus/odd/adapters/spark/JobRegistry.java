package com.provectus.odd.adapters.spark;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;

@Slf4j
public class JobRegistry {
    final WeakHashMap<RDD<?>, JobInfo> rddJobMap = new WeakHashMap<>();
    final WeakHashMap<Integer, JobInfo> jobIdJobMap = new WeakHashMap<>();

    /**
     * Entry point for PairRDDFunctionsTransformer
     *
     * <p>called through the agent when writing with the RDD API as the RDDs do not contain the output
     * information
     *
     * @see PairRDDFunctionsTransformer
     * @param pairRDDFunctions the wrapping RDD containing the rdd to save
     * @param conf the write config
     */
    public void registerOutput(PairRDDFunctions<?, ?> pairRDDFunctions, Configuration conf) {
        try {
            log.info("Searching for outputs...");
            Field[] declaredFields = pairRDDFunctions.getClass().getDeclaredFields();
            for (Field field : declaredFields) {
                if (field.getName().endsWith("self") && RDD.class.isAssignableFrom(field.getType())) {
                    field.setAccessible(true);
                    try {
                        RDD<?> rdd = (RDD<?>) field.get(pairRDDFunctions);
                        doRegisterOutput(conf, rdd);
                    } catch (IllegalArgumentException | IllegalAccessException e) {
                        e.printStackTrace(System.out);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Could not register output", e);
//            SparkListener.emitError(e); // TODO
        }
        log.info("done searching.");
    }

    private void doRegisterOutput(Configuration conf, RDD<?> rdd) {
        log.info("found output {}, {}", conf, rdd);
        JobInfo jobInfo = rddJobMap.getOrDefault(rdd, new JobInfo());
        jobInfo.setConf(conf);
        rddJobMap.put(rdd, jobInfo);
    }

    protected Path getOutputPath(RDD<?> rdd) {
        Configuration conf = getConfigForRDD(rdd);
        if (conf == null) {
            return null;
        }
        // "new" mapred api
        Path path = org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(new JobConf(conf));
        if (path == null) {
            try {
                // old fashioned mapreduce api
                path = org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.getOutputPath(new Job(conf));
            } catch (IOException exception) {
                log.error("Error getting job output path", exception);
            }
        }
        return path;
    }

    private Configuration getConfigForRDD(RDD<?> rdd) {
        JobInfo jobInfo = rddJobMap.get(rdd);
        return jobInfo == null ? null : jobInfo.getConf();
    }

    protected List<URI> findOutputs(Set<RDD<?>> rdds, @NonNull SparkListener sparkListener) {
        if (rdds == null) {
            return null;
        }
        List<URI> result = new ArrayList<>();
        for (RDD<?> rdd : rdds) {
            Path outputPath = getOutputPath(rdd);
            if (outputPath != null) {
                result.add(sparkListener.getDatasetUri(outputPath.toUri()));
            }
        }
        return result;
    }

    public void registerJob(int jobId, JobInfo jobInfo) {
        jobIdJobMap.put(jobId, jobInfo);
    }

    public void removeJob(int jobId) {
        jobIdJobMap.remove(jobId);
    }

    public JobInfo getJobById(int jobId) {
        return jobIdJobMap.get(jobId);
    }
}
