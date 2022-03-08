package com.wsk.spark.cor

import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * 自定义的逻辑计划优化规则
 */
object MultiplyOptimizationRule extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
        case Multiply(left, right, _) if right.isInstanceOf[Literal]
                && right.asInstanceOf[Literal].value.asInstanceOf[Long] == 1 => left
        case Multiply(left, right, _) if left.isInstanceOf[Literal]
                && left.asInstanceOf[Literal].value.asInstanceOf[Long] == 1 => right
    }

    // (重要)上述的方法是scala 偏函数 简写的，比较难以看懂，可以通过如下方法简写，
    def apply2(plan: LogicalPlan): LogicalPlan = {
        plan.transformAllExpressions({
            case Multiply(left, right, _) if right.isInstanceOf[Literal]
                    && right.asInstanceOf[Literal].value.asInstanceOf[Long] == 1 => left
            case Multiply(left, right, _) if left.isInstanceOf[Literal]
                    && left.asInstanceOf[Literal].value.asInstanceOf[Long] == 1 => right
        })
    }
}
