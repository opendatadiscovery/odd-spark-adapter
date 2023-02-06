//package org.opendatadiscovery.adapters.spark.plan;
//
//import lombok.extern.slf4j.Slf4j;
//import net.snowflake.spark.snowflake.SnowflakeRelation;
//import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
//import org.apache.spark.sql.execution.datasources.LogicalRelation;
//import org.opendatadiscovery.client.model.DataEntity;
//
//import java.util.Collections;
//import java.util.List;
//
//@Slf4j
//public class SnowflakeRelationVisitor extends QueryPlanVisitor {
//    private static final String SNOWFLAKE_CLASS_NAME = "net.snowflake.spark.snowflake.SnowflakeRelation";
//    private static final String SNOWFLAKE_PROVIDER_CLASS_NAME = "net.snowflake.spark.snowflake.DefaultSource";
//
//    @Override
//    public boolean isDefinedAt(final LogicalPlan logicalPlan) {
//        try {
//            Class c = Thread.currentThread().getContextClassLoader().loadClass(SNOWFLAKE_CLASS_NAME);
//            return (logicalPlan instanceof LogicalRelation
//                && c.isAssignableFrom(((LogicalRelation) logicalPlan).relation().getClass()));
//        } catch (Exception e) {
//            log.info(e.getMessage());
//            // swallow - not a snowflake class
//        }
//        return false;
//    }
//
//    @Override
//    public List<String> apply(final LogicalPlan x) {
//        final SnowflakeRelation relation = (SnowflakeRelation) ((LogicalRelation) x).relation();
//        log.info("Relation parameters: {}", relation.params().parameters());
//        return Collections.singletonList("test");
//    }
//}
