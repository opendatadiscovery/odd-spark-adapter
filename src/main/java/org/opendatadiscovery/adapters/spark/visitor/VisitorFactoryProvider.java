package org.opendatadiscovery.adapters.spark.visitor;

import lombok.experimental.UtilityClass;
import org.apache.spark.SparkContext;

@UtilityClass
public class VisitorFactoryProvider {
    public static VisitorFactory create(final SparkContext sparkContext) {
        return new VisitorFactoryImpl(sparkContext);
    }
}
