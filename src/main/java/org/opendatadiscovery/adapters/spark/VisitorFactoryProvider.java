package org.opendatadiscovery.adapters.spark;

import org.apache.spark.sql.SparkSession;

class VisitorFactoryProvider {

  static VisitorFactory getInstance(SparkSession session) {
    return new VisitorFactoryImpl();
  }
}
