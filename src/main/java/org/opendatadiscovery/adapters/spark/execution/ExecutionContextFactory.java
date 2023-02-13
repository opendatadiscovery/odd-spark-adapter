package org.opendatadiscovery.adapters.spark.execution;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.opendatadiscovery.adapters.spark.dto.ExecutionPayload;
import scala.Option;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.apache.commons.validator.routines.UrlValidator.ALLOW_LOCAL_URLS;

@UtilityClass
@Slf4j
public class ExecutionContextFactory {
    private static final UrlValidator URL_VALIDATOR = new UrlValidator(new String[]{"http", "https"}, ALLOW_LOCAL_URLS);

    private static final String ODD_ENABLED_ENTRY = "spark.odd.enabled";
    private static final String ODD_HOST_ENTRY = "spark.odd.host.url";
    private static final String ODD_ODDRN_KEY = "spark.odd.oddrn.key";

    public static ExecutionContext create(final String applicationName) {
        final OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        final SparkConf sparkConf = SparkEnv.get().conf();

        final Option<String> oddrnKey = sparkConf.getOption(ODD_ODDRN_KEY);

        if (oddrnKey.isEmpty()) {
            log.warn("{} property is not set. Spark Listener will not collect metadata", ODD_ODDRN_KEY);
            return new LoggingExecutionContext(new ExecutionPayload(applicationName, "UNKNOWN", now));
        }

        final boolean enabled = sparkConf.getOption(ODD_ENABLED_ENTRY)
            .map(Boolean::parseBoolean)
            .getOrElse(() -> true);

        final ExecutionPayload payload = new ExecutionPayload(applicationName, oddrnKey.get(), now);

        if (!enabled) {
            log.warn("{} property is not equal to true. Spark Listener will not collect metadata", ODD_ENABLED_ENTRY);
            return new LoggingExecutionContext(payload);
        }

        // TODO: validate ODD Platform URL
        final Option<String> oddPlatformUrl = sparkConf
            .getOption(ODD_HOST_ENTRY)
            .filter(URL_VALIDATOR::isValid);

        if (oddPlatformUrl.isEmpty()) {
            log.warn("Couldn't find or validate ODD Platform URL from {} property. " +
                "Spark Listener will not collect metadata", ODD_HOST_ENTRY);
            return new LoggingExecutionContext(payload);
        }

        return new HttpExecutionContext(payload, oddPlatformUrl.get());
    }
}
