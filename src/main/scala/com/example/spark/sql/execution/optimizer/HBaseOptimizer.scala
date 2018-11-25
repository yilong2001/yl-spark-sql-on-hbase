package com.example.spark.sql.execution.optimizer

import com.example.spark.sql.execution.hbase.HBaseRelation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.expressions._

/**
  * Created by yilong on 2018/9/15.
  */

object HBaseStatisOptimizationRule extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f @ Filter(condition, child) =>
      println("*************************** : "+condition.toString())
      //println(child.toJSON)
      if (child.isInstanceOf[HBaseRelation]) print("Filter has HBaseRelation as child")
      if (condition.deterministic) {
        f
      } else {
        f
      }
  }
}
