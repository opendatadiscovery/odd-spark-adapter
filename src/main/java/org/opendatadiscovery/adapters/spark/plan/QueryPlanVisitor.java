package org.opendatadiscovery.adapters.spark.plan;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.adapters.spark.dto.LogicalPlanDependencies;
import scala.runtime.AbstractPartialFunction;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class QueryPlanVisitor<T extends LogicalPlan>
    extends AbstractPartialFunction<LogicalPlan, LogicalPlanDependencies> {
    @Override
    public boolean isDefinedAt(final LogicalPlan logicalPlan) {
        final Type genericSuperclass = getClass().getGenericSuperclass();
        if (!(genericSuperclass instanceof ParameterizedType)) {
            return false;
        }
        final Type[] typeArgs = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
        if (typeArgs != null && typeArgs.length > 0) {
            final Type arg = typeArgs[0];
            return ((Class) arg).isAssignableFrom(logicalPlan.getClass());
        }
        return false;
    }
}