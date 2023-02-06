package org.opendatadiscovery.adapters.spark.utils;

import lombok.experimental.UtilityClass;
import org.apache.spark.SparkContext$;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.sql.SparkSession;
import scala.Option;

import java.util.Optional;

import static org.opendatadiscovery.adapters.spark.utils.ScalaConversionUtils.asJavaOptional;

@UtilityClass
public class SparkUtils {
    public static Optional<ActiveJob> getActiveJob(final long jobId) {
        final Option<ActiveJob> activeJob = SparkSession.getDefaultSession()
            .map(SparkSession::sparkContext)
            .orElse(SparkContext$.MODULE$::getActive)
            .flatMap(ctx -> Option.apply(ctx.dagScheduler()))
            .flatMap(ds -> ds.jobIdToActiveJob().get(jobId));

        return asJavaOptional(activeJob);
    }
}
