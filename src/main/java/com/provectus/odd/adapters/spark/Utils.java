package com.provectus.odd.adapters.spark;

import org.apache.spark.Dependency;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import scala.Function1;
import scala.Option;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Utils {

    private static final ObjectMapper jacksonMapper = new ObjectMapper();

    static {
        jacksonMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
        jacksonMapper.configure(SerializationConfig.Feature.USE_ANNOTATIONS, true);
    }

    public static final DateTimeFormatter isoFormatter = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral('T')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .append(DateTimeFormatter.ofPattern("Z"))
            .toFormatter(Locale.getDefault());

    public static Set<RDD<?>> flattenRDDs(RDD<?> rdd) {
        Set<RDD<?>> rdds = new HashSet<>();
        rdds.add(rdd);
        Collection<Dependency<?>> deps = JavaConversions.asJavaCollection(rdd.dependencies());
        for (Dependency<?> dep : deps) {
            rdds.addAll(flattenRDDs(dep.rdd()));
        }
        return rdds;
    }

    public static <T, R> Function1<T, R> toScalaFn(Function<T, R> fn) {
        return new AbstractFunction1<T, R>() {
            @Override
            public R apply(T arg) {
                return fn.apply(arg);
            }
        };
    }

    public static <T> Optional<T> asJavaOptional(Option<T> opt) {
        return Optional.ofNullable(opt.getOrElse(() -> null));
    }

    static String activeJobToString(ActiveJob activeJob) {
//        CallSite callSite = activeJob.callSite();
        String callSite = "<omitted>";
        return String.format("ActiveJob(jobId=%s, finalStage=%s, callSite=%s, listener=%s, properties=%s)",
                activeJob.jobId(), activeJob.finalStage(), callSite, activeJob.listener(),
                activeJob.properties());
    }

    static List<String> toStringList(List<URI> outputs) {
        return outputs.stream().map(URI::toString).collect(Collectors.toList());
    }

    static String toJsonString(Object o) {
        String json;
        try {
            json = jacksonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(o);
        } catch (IOException e) {
            e.printStackTrace();
            json = e.toString();
        }
        return json;
    }

    static String timestampToString(long time) {
        return isoFormatter.format(new Date(time).toInstant().atOffset(ZoneOffset.UTC));
    }
}