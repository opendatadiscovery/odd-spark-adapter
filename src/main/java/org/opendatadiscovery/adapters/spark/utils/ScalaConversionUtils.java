package org.opendatadiscovery.adapters.spark.utils;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import scala.Function0;
import scala.Function1;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

/** Simple conversion utilities for dealing with Scala types. */
public class ScalaConversionUtils {

    /**
     * Apply a map function to a {@link Seq}. This consolidates the silliness in converting between
     * Scala and Java collections.
     */
    public static <R, T> Seq<R> map(final Seq<T> seq, final Function<T, R> fn) {
        return fromList(fromSeq(seq).stream().map(fn).collect(Collectors.toList()));
    }

    /**
     * Convert a {@link List} to a Scala {@link Seq}.
     */
    public static <T> Seq<T> fromList(final List<T> list) {
        return JavaConverters.asScalaBufferConverter(list).asScala();
    }

    /**
     * Convert a {@link Seq} to a Java {@link List}.
     */
    public static <T> List<T> fromSeq(final Seq<T> seq) {
        return JavaConverters.bufferAsJavaListConverter(seq.<T>toBuffer()).asJava();
    }

    /**
     * Convert a Scala {@link Option} to a Java {@link Optional}.
     */
    public static <T> Optional<T> asJavaOptional(final Option<T> opt) {
        return Optional.ofNullable(
                opt.getOrElse(
                        new AbstractFunction0<T>() {
                            @Override
                            public T apply() {
                                return null;
                            }
                        }));
    }

    /**
     * Convert a {@link Supplier} to a Scala {@link Function0}.
     */
    public static <T> Function0<T> toScalaFn(final Supplier<T> supplier) {
        return new AbstractFunction0<T>() {
            @Override
            public T apply() {
                return supplier.get();
            }
        };
    }

    /**
     * Convert a {@link Function} to a Scala {@link scala.Function1}.
     */
    public static <T, R> Function1<T, R> toScalaFn(final Function<T, R> fn) {
        return new AbstractFunction1<T, R>() {
            @Override
            public R apply(final T arg) {
                return fn.apply(arg);
            }
        };
    }

    public static Optional<String> findSparkConfigKey(final SparkConf conf, final String name) {
        return ScalaConversionUtils.asJavaOptional(
                conf.getOption(name)
                        .getOrElse(toScalaFn(() -> conf.getOption("spark." + name))));
    }
}
