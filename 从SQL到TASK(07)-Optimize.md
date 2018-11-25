#

## QueryExecution
QueryExecution 是 Spark 执行 SQL查询的主要实现。定义了开放式插件接口，用户可自定义Rule和Strategy

### optimize rule set
(1) 执行 optimizer
```
  lazy val optimizedPlan: LogicalPlan = 
  sparkSession.sessionState.optimizer.execute(withCachedData)

```

(2) Optimizer Rule Set in (Optimizer)
```
abstract class Optimizer(sessionCatalog: SessionCatalog)
  extends RuleExecutor[LogicalPlan] {

  // Check for structural integrity of the plan in test mode. Currently we only check if a plan is
  // still resolved after the execution of each rule.
  override protected def isPlanIntegral(plan: LogicalPlan): Boolean = {
    !Utils.isTesting || plan.resolved
  }

  protected def fixedPoint = FixedPoint(SQLConf.get.optimizerMaxIterations)

  def batches: Seq[Batch] = {
    val operatorOptimizationRuleSet =
      Seq(
        // Operator push down
        PushProjectionThroughUnion,
        ReorderJoin,
        EliminateOuterJoin,
        PushPredicateThroughJoin,
        PushDownPredicate,
        LimitPushDown,
        ColumnPruning,
        InferFiltersFromConstraints,
        // Operator combine
        CollapseRepartition,
        CollapseProject,
        CollapseWindow,
        CombineFilters,
        CombineLimits,
        CombineUnions,
        // Constant folding and strength reduction
        NullPropagation,
        ConstantPropagation,
        FoldablePropagation,
        OptimizeIn,
        ConstantFolding,
        ReorderAssociativeOperator,
        LikeSimplification,
        BooleanSimplification,
        SimplifyConditionals,
        RemoveDispensableExpressions,
        SimplifyBinaryComparison,
        PruneFilters,
        EliminateSorts,
        SimplifyCasts,
        SimplifyCaseConversionExpressions,
        RewriteCorrelatedScalarSubquery,
        EliminateSerialization,
        RemoveRedundantAliases,
        RemoveRedundantProject,
        SimplifyCreateStructOps,
        SimplifyCreateArrayOps,
        SimplifyCreateMapOps,
        CombineConcats) ++
        extendedOperatorOptimizationRules

    val operatorOptimizationBatch: Seq[Batch] = {
      val rulesWithoutInferFiltersFromConstraints =
        operatorOptimizationRuleSet.filterNot(_ == InferFiltersFromConstraints)
      Batch("Operator Optimization before Inferring Filters", fixedPoint,
        rulesWithoutInferFiltersFromConstraints: _*) ::
      Batch("Infer Filters", Once,
        InferFiltersFromConstraints) ::
      Batch("Operator Optimization after Inferring Filters", fixedPoint,
        rulesWithoutInferFiltersFromConstraints: _*) :: Nil
    }

    (Batch("Eliminate Distinct", Once, EliminateDistinct) ::
    // Technically some of the rules in Finish Analysis are not optimizer rules and belong more
    // in the analyzer, because they are needed for correctness (e.g. ComputeCurrentTime).
    // However, because we also use the analyzer to canonicalized queries (for view definition),
    // we do not eliminate subqueries or compute current time in the analyzer.
    Batch("Finish Analysis", Once,
      EliminateSubqueryAliases,
      EliminateView,
      ReplaceExpressions,
      ComputeCurrentTime,
      GetCurrentDatabase(sessionCatalog),
      RewriteDistinctAggregates,
      ReplaceDeduplicateWithAggregate) ::
    //////////////////////////////////////////////////////////////////////////////////////////
    // Optimizer rules start here
    //////////////////////////////////////////////////////////////////////////////////////////
    // - Do the first call of CombineUnions before starting the major Optimizer rules,
    //   since it can reduce the number of iteration and the other rules could add/move
    //   extra operators between two adjacent Union operators.
    // - Call CombineUnions again in Batch("Operator Optimizations"),
    //   since the other rules might make two separate Unions operators adjacent.
    Batch("Union", Once,
      CombineUnions) ::
    Batch("Pullup Correlated Expressions", Once,
      PullupCorrelatedPredicates) ::
    Batch("Subquery", Once,
      OptimizeSubqueries) ::
    Batch("Replace Operators", fixedPoint,
      ReplaceIntersectWithSemiJoin,
      ReplaceExceptWithFilter,
      ReplaceExceptWithAntiJoin,
      ReplaceDistinctWithAggregate) ::
    Batch("Aggregate", fixedPoint,
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions) :: Nil ++
    operatorOptimizationBatch) :+
    Batch("Join Reorder", Once,
      CostBasedJoinReorder) :+
    Batch("Decimal Optimizations", fixedPoint,
      DecimalAggregates) :+
    Batch("Object Expressions Optimization", fixedPoint,
      EliminateMapObjects,
      CombineTypedFilters) :+
    Batch("LocalRelation", fixedPoint,
      ConvertToLocalRelation,
      PropagateEmptyRelation) :+
    // The following batch should be executed after batch "Join Reorder" and "LocalRelation".
    Batch("Check Cartesian Products", Once,
      CheckCartesianProducts) :+
    Batch("RewriteSubquery", Once,
      RewritePredicateSubquery,
      ColumnPruning,
      CollapseProject,
      RemoveRedundantProject)
  }


```

(3)  Optimizer Rule Set in (SparkOptimizer)
```
class SparkOptimizer(
    catalog: SessionCatalog,
    experimentalMethods: ExperimentalMethods)
  extends Optimizer(catalog) {

  override def batches: Seq[Batch] = (preOptimizationBatches ++ super.batches :+
    Batch("Optimize Metadata Only Query", Once, OptimizeMetadataOnlyQuery(catalog)) :+
    Batch("Extract Python UDF from Aggregate", Once, ExtractPythonUDFFromAggregate) :+
    Batch("Prune File Source Table Partitions", Once, PruneFileSourcePartitions) :+
    Batch("Push down operators to data source scan", Once, PushDownOperatorsToDataSource)) ++
    postHocOptimizationBatches :+
    Batch("User Provided Optimizers", fixedPoint, experimentalMethods.extraOptimizations: _*)

  /**
   * Optimization batches that are executed before the regular optimization batches (also before
   * the finish analysis batch).
   */
  def preOptimizationBatches: Seq[Batch] = Nil

  /**
   * Optimization batches that are executed after the regular optimization batches, but before the
   * batch executing the [[ExperimentalMethods]] optimizer rules. This hook can be used to add
   * custom optimizer batches to the Spark optimizer.
   */
   def postHocOptimizationBatches: Seq[Batch] = Nil
}
```

### Rules
(1) Push Projection Through Union
投影下推到 Union 的两侧。Union 即 UNION ALL，不做 rows 的去重。因此，下推 投影和谓词 是安全的。
但如果是 UNION DISTINCT，则不能做投影下推。

```
 * Pushes Project operator to both sides of a Union operator.
 * Operations that are safe to pushdown are listed as follows.
 * Union:
 * Right now, Union means UNION ALL, which does not de-duplicate rows. So, it is
 * safe to pushdown Filters and Projections through it. Filter pushdown is handled by another
 * rule PushDownPredicate. Once we add UNION DISTINCT, we will not be able to pushdown Projections.

```

(2) Reorder Join
Joins 重排，并且把所有条件下推到 join，因此 join 的数据量将最小化（最下一层的 join 的数据量最小）。
```
 * Reorder the joins and push all the conditions into join, so that the bottom ones have at least
 * one condition.
 *
 * The order of joins will not be changed if all of them already have at least one condition.
 *
 * If star schema detection is enabled, reorder the star join plans based on heuristics.

```

(3) Push Predicate Through Join : 谓词下推到 Join 

(4) Push Down Predicate : 谓词下推到 Relation

(5) Limit Push Down : Limit 条件下推

(6) Column Pruning : 列裁剪

(7) Infer Filters From Constraints : 从操作符的限制条件，生成新的 Filters(下推)，主要应用于 Inner Joins、LeftSemi joins。

(8) Null Propagation : 空值传播, 使用默认值代替 Null 值

(9) Constant Propagation : 常量传播，使用值代替变量名称

(10) Foldable Propagation : 折叠传播
```
 * Replace attributes with aliases of the original foldable expressions if possible.
 * Other optimizations will take advantage of the propagated foldable expressions. For example,
 * this rule can optimize
 * {{{
 *   SELECT 1.0 x, 'abc' y, Now() z ORDER BY x, y, 3
 * }}}
 * to
 * {{{
 *   SELECT 1.0 x, 'abc' y, Now() z ORDER BY 1.0, 'abc', Now()
 * }}}
 * and other rules can further optimize it and remove the ORDER BY operator.
```

(11) Constant Folding : 常量折叠，计算常量表达式

### CBO vs RBO
(12) Cost Based Join Reorder(CBO) : 基于代价的join重排; RBO: Rule Based Join Reorder

#### 前提
cbo Enabled && join Reorder Enabled

#### 算法(IO + CPU，network???)
SQL 中常见的操作有 Selection（由 select 语句表示），Filter（由 where 语句表示）以及笛卡尔乘积（由 join 语句表示）。其中代价最高的是 join。
Spark SQL 的 CBO 通过如下方法估算 join 的代价:

即：IO代价 + CPU代价 加权组成整体 join 的代价
```
Cost = rows * weight + size * (1 - weight)
Cost = CostCPU * weight + CostIO * (1 - weight)
```
当然，如果考虑到sort时候，数据通过网络传输的问题，还应该加上 network cost，不过当然CBO算法没有考虑这一点。

#### 影响代价的因素
1) IO Cost
IO Cost与从磁盘读取的数据量(byte size)有关系。在相同 rows 的条件下，平均 byte size 越小，cost越小。
这就是列裁剪下推、谓词下推的价值所在。从根本上减少读取的数据量。

除此之外，是否有索引也影响 IO Cost。对于有索引的记录，只需要读取Index + Segment即能得到需要查询的数据内容；否则，就需要做 全表scan。
对于 hbase table scan 而言，全表 scan 的 cost 显然非常大。

2) CPU Cost
CPU Cost 与内存中计算的记录数有关(row number)。同样，谓词下推也是为了减少要计算的 row number。

#### 估算数据量
对于 = > < between range in 等算法，如何估算可能的数据量(?)，其算法如下：
```
* column = value
F = 1 / ICARD(column index) if there is an index on column
This assumes an even distribution of tuples among the index key values. F = 1/10 otherwise

* column1 = column2
F = 1/MAX(ICARD(column1 index), ICARD(column2 index))
if there are indexes on both column1 and column2
This assumes that each key value in the index with the smaller cardinality has a matching value in the other index.
F = 1/ICARD(column-i index) if there is only an index on column-i
F = 1/10 otherwise

* column > value (or any other open-ended comparison)
F = (high key value - value) / (high key value - low key value)
Linear interpolation of the value within the range of key values yields F if the col- umn is an arithmetic type and value is known at access path selection time.
F = 1/3 otherwise (i.e. column not arithmetic)
There is no significance to this number, other than the fact that it is less selec- tive than the guesses for equal predicates for which there are no indexes, and that it is less than 1/2. We hypothesize that few queries use predicates that are satis- fied by more than half the tuples.

* column BETWEEN value1 AND value2
F = (value2 - value1) / (high key value - low key value)
A ratio of the BETWEEN value range to the entire key value range is used as the selectivity factor if column is arithmetic and both value1 and value2 are known at access path selection.
F = 1/4 otherwise
Again there is no significance to this choice except that it is between the default selectivity factors for an equal predicate and a range predicate.

* column IN (list of values)
F = (number of items in list) * (selectivity factor for column = value) This is allowed to be no more than 1/2.

* columnA IN subquery
F = (expected cardinality of the subquery result) / (product of the cardinalities of
all the relations in the subquery’s FROM-list).
The computation of query cardinality will be discussed below. This formula is derived by the following argument:
Consider the simplest case, where subquery is of the form “SELECT columnB FROM rela- tionC ...”. Assume that the set of all columnB values in relationC contains the set of all columnA values. If all the tuples of relationC are selected by the subquery, then the predicate is always TRUE and F = 1. If the tuples of the subquery are restricted by a selectivity factor F’, then assume that the set of unique values in the subquery result that match columnA values is proportionately restricted, i.e. the selectivity factor for the predicate should be F’. F’ is the product of all the sub- query’s selectivity factors, namely (subquery cardinality) / (cardinality of all pos- sible subquery answers). With a little optimism, we can extend this reasoning to include subqueries which are joins and subqueries in which columnB is replaced by an arithmetic expression involving column names. This leads to the formula given above.

* (pred expression1) OR (pred expression2)
F = F(pred1) + F(pred2) - F(pred1) * F(pred2)

* (pred1) AND (pred2)
F = F(pred1) * F(pred2)
Note that this assumes that column values are independent.

* NOT pred
F = 1 - F(pred)
```

#### 估算优化
以上估算方法假设数据是平均分布的，如果数据不是平均分布，则估计结果不准确。
要更准确的估计数据量，更精确的方法是使用直方图。如果数据源有直方图，则有助于更准确的执行CBO。

#### 计算过程
spark CBO 用动态规划算法执行优化。过程如下：

```
 * First we put all items (basic joined nodes) into level 0, then we build all two-way joins
 * at level 1 from plans at level 0 (single items), then build all 3-way joins from plans
 * at previous levels (two-way joins and single items), then 4-way joins ... etc, until we
 * build all n-way joins and pick the best plan among them.
 *
 * When building m-way joins, we only keep the best plan (with the lowest cost) for the same set
 * of m items. E.g., for 3-way joins, we keep only the best plan for items {A, B, C} among
 * plans (A J B) J C, (A J C) J B and (B J C) J A.
 * We also prune cartesian product candidates when building a new plan if there exists no join
 * condition involving references from both left and right. This pruning strategy significantly
 * reduces the search space.
 * E.g., given A J B J C J D with join conditions A.k1 = B.k1 and B.k2 = C.k2 and C.k3 = D.k3,
 * plans maintained for each level are as follows:
 * level 0: p({A}), p({B}), p({C}), p({D})
 * level 1: p({A, B}), p({B, C}), p({C, D})
 * level 2: p({A, B, C}), p({B, C, D})
 * level 3: p({A, B, C, D})
 * where p({A, B, C, D}) is the final output plan.

```
