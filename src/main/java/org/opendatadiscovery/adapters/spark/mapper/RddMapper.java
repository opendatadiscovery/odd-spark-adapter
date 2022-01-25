package org.opendatadiscovery.adapters.spark.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.Strings;
import org.apache.spark.Dependency;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.io.HadoopMapRedWriteConfigUtil;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.ResultStage;
import org.apache.spark.util.SerializableJobConf;
import org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils;
import scala.Function2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.List;
import java.util.Optional;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;

import static org.opendatadiscovery.adapters.spark.utils.Utils.CAMEL_TO_SNAKE_CASE;

@Slf4j
public class RddMapper {

    public String name(RDD<?> rdd) {
        String rddName = rdd.name();
        if (rddName == null
                // HadoopRDDs are always named for the path. Don't name the RDD for a file. Otherwise, the
                // job name will end up differing each time we read a path with a date or other variable
                // directory name
                || (rdd instanceof HadoopRDD
                && Arrays.stream(FileInputFormat.getInputPaths(((HadoopRDD) rdd).getJobConf()))
                .anyMatch(p -> p.toString().contains(rdd.name())))
                // If the map RDD is named the same as its dependent, just use map_partition
                // This happens, e.g., when calling sparkContext.textFile(), as it creates a HadoopRDD, maps
                // the value to a string, and sets the name of the mapped RDD to the path, which is already
                // the name of the underlying HadoopRDD
                || (rdd instanceof MapPartitionsRDD
                && rdd.name().equals(((MapPartitionsRDD) rdd).prev().name()))) {
            rddName =
                    rdd.getClass()
                            .getSimpleName()
                            .replaceAll("RDD\\d*$", "") // remove the trailing RDD from the class name
                            .replaceAll(CAMEL_TO_SNAKE_CASE, "_$1") // camel case to snake case
                            .toLowerCase(Locale.ROOT);
        }
        Seq<Dependency<?>> deps = rdd.dependencies();
        List<Dependency<?>> dependencies = ScalaConversionUtils.fromSeq(deps);
        if (dependencies.isEmpty()) {
            return rddName;
        }
        List<String> dependencyNames = dependencies.stream()
                .map(d -> name(d.rdd()))
                .collect(Collectors.toList());
        String dependencyName = Strings.join(dependencyNames, "_");
        if (!dependencyName.startsWith(rddName)) {
            return rddName + "_" + dependencyName;
        } else {
            return dependencyName;
        }
    }

    public List<URI> inputs(RDD<?> finalRdd) {
        Set<RDD<?>> rdds = flatten(finalRdd);
        return rdds.stream()
                .map(this::getInputPaths)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .map(Path::toUri)
                .collect(Collectors.toList());
    }

    public List<URI> outputs(ActiveJob job, Configuration config) {
        Configuration jc = new JobConf();
        if (job.finalStage() instanceof ResultStage) {
            Function2<TaskContext, Iterator<?>, ?> fn = ((ResultStage) job.finalStage()).func();
            try {
                Field f = getConfigField(fn);
                f.setAccessible(true);

                HadoopMapRedWriteConfigUtil configUtil =
                        Optional.of(f.get(fn))
                                .filter(HadoopMapRedWriteConfigUtil.class::isInstance)
                                .map(HadoopMapRedWriteConfigUtil.class::cast)
                                .orElseThrow(
                                        () ->
                                                new NoSuchFieldException(
                                                        "Field is not instance of HadoopMapRedWriteConfigUtil"));

                Field confField = HadoopMapRedWriteConfigUtil.class.getDeclaredField("conf");
                confField.setAccessible(true);
                SerializableJobConf conf = (SerializableJobConf) confField.get(configUtil);
                jc = conf.value();
            } catch (IllegalAccessException | NoSuchFieldException nfe) {
                log.warn("Unable to access job conf from RDD", nfe);
            }
            log.info("Found job conf from RDD {}", jc);
        } else {
            jc = config;
        }
        Path outputPath = getOutputPath(jc);
        log.info("Found output path {} from RDD {}", outputPath, job.finalStage().rdd());
        if (outputPath != null) {
            return Collections.singletonList(outputPath.toUri());
        }
        return Collections.emptyList();
    }

    private Path getOutputPath(Configuration config) {
        if (config == null) {
            return null;
        }
        JobConf jc;
        if (config instanceof JobConf) {
            jc = (JobConf) config;
        } else {
            jc = new JobConf(config);
        }
        Path path = org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(jc);
        if (path == null) {
            try {
                // old fashioned mapreduce api
                path = org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.getOutputPath(new Job(jc));
            } catch (IOException e) {
                log.warn("", e);
            }
        }
        return path;
    }

    private Field getConfigField(Function2<TaskContext, scala.collection.Iterator<?>, ?> fn)
            throws NoSuchFieldException {
        try {
            return fn.getClass().getDeclaredField("config$1");
        } catch (NoSuchFieldException e) {
            return fn.getClass().getDeclaredField("arg$1");
        }
    }

    private Set<RDD<?>> flatten(RDD<?> rdd) {
        Set<RDD<?>> rdds = new HashSet<>();
        rdds.add(rdd);
        Collection<Dependency<?>> deps = JavaConversions.asJavaCollection(rdd.dependencies());
        for (Dependency<?> dep : deps) {
            rdds.addAll(flatten(dep.rdd()));
        }
        return rdds;
    }

    private List<Path> getInputPaths(RDD<?> rdd) {
        if (rdd instanceof HadoopRDD) {
            return Arrays.asList(
                    org.apache.hadoop.mapred.FileInputFormat.getInputPaths(
                            ((HadoopRDD<?, ?>) rdd).getJobConf()));
        } else if (rdd instanceof NewHadoopRDD) {
            try {
                return Arrays.asList(
                        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.getInputPaths(
                                new Job(((NewHadoopRDD<?, ?>) rdd).getConf())));
            } catch (IOException e) {
                log.error("ODD spark agent could not get input paths", e);
            }
        }
        return null;
    }
}
