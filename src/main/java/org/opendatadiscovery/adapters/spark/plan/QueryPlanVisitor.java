package org.opendatadiscovery.adapters.spark.plan;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opendatadiscovery.client.model.DataEntity;
import scala.runtime.AbstractPartialFunction;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

public abstract class QueryPlanVisitor<T extends LogicalPlan, D extends DataEntity>
    extends AbstractPartialFunction<LogicalPlan, List<D>> {

  @Override
  public boolean isDefinedAt(LogicalPlan logicalPlan) {
    Type genericSuperclass = getClass().getGenericSuperclass();
    if (!(genericSuperclass instanceof ParameterizedType)) {
      return false;
    }
    Type[] typeArgs = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
    if (typeArgs != null && typeArgs.length > 0) {
      Type arg = typeArgs[0];
      return ((Class) arg).isAssignableFrom(logicalPlan.getClass());
    }
    return false;
  }
}