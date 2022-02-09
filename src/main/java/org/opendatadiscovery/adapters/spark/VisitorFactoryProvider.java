package org.opendatadiscovery.adapters.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;

@Slf4j
class VisitorFactoryProvider {

    static VisitorFactory getInstance(final SparkContext context) {
        log.info("Spark version: {}", context.version());
        return new VisitorFactoryImpl();
    }
}
