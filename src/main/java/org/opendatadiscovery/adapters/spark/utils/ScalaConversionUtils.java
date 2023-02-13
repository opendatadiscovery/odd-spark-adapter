package org.opendatadiscovery.adapters.spark.utils;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Optional;

public class ScalaConversionUtils {
    public static <T> List<T> fromSeq(final Seq<T> seq) {
        return JavaConverters.bufferAsJavaListConverter(seq.<T>toBuffer()).asJava();
    }

    public static <T> Optional<T> asJavaOptional(final Option<T> opt) {
        return Optional.ofNullable(opt.getOrElse(() -> null));
    }
}
