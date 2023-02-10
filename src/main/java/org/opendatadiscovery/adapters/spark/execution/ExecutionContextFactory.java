package org.opendatadiscovery.adapters.spark.execution;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import scala.Option;

@UtilityClass
@Slf4j
public class ExecutionContextFactory {
    private static final UrlValidator URL_VALIDATOR = new UrlValidator(new String[]{"http", "https"});

    private static final String ODD_ENABLED_ENTRY = "spark.odd.enabled";
    private static final String ODD_HOST_ENTRY = "spark.odd.host.url";

    public static ExecutionContext create() {
        final SparkConf sparkConf = SparkEnv.get().conf();

        final boolean enabled = sparkConf.getOption(ODD_ENABLED_ENTRY)
            .map(Boolean::parseBoolean)
            .getOrElse(() -> true);

        if (!enabled) {
            log.warn("{} property is not equal to true. Spark Listener will not collect metadata", ODD_ENABLED_ENTRY);
            return new NoOpExecutionContext();
        }

        final Option<String> oddPlatformUrl = sparkConf.getOption(ODD_HOST_ENTRY).filter(URL_VALIDATOR::isValid);

        if (oddPlatformUrl.isEmpty()) {
            log.warn("Couldn't find or validate ODD Platform URL from {} property. " +
                "Spark Listener will not collect metadata", ODD_HOST_ENTRY);
            return new NoOpExecutionContext();
        }

        return new ExecutionContextImpl(oddPlatformUrl.get());
    }
}
