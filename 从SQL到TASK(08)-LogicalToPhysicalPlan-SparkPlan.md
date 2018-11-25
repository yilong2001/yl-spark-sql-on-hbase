#

## SparkPlan <-- QueryPlanner
SparkPlan 完成从 logical plan to physical plan 的转换。当然，转换的过程也是不断使用预定策略来完成。

### QueryPlanner.plan (logical to physical)
如下，转换过程中递归使用 Seq[GenericStrategy[PhysicalPlan]]。除了系统设定的 Strategy ，也支持用户自定义的 Strategy 。
通过设置 ExperimentalMethods 自定义策略和规则。参考(SessionState)。

```
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategy[PhysicalPlan]]

  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...

    // Collect physical plan candidates.
    val candidates = strategies.iterator.flatMap(_(plan))

    // The candidates may contain placeholders marked as [[planLater]],
    // so try to replace them by their child plans.
    val plans = candidates.flatMap { candidate =>
      val placeholders = collectPlaceholders(candidate)

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Plan the logical plan for the placeholder.
            val childPlans = this.plan(logicalPlan)

            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  case p if p == placeholder => childPlan
                }
              }
            }
        }
      }
    }

    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")
    pruned
  }

  /**
   * Collects placeholders marked using [[GenericStrategy#planLater planLater]]
   * by [[strategies]].
   */
  protected def collectPlaceholders(plan: PhysicalPlan): Seq[(PhysicalPlan, LogicalPlan)]

  /** Prunes bad plans to prevent combinatorial explosion. */
  protected def prunePlans(plans: Iterator[PhysicalPlan]): Iterator[PhysicalPlan]
}

```


### Strategies in SparkPlan
```
  override def strategies: Seq[Strategy] =
    experimentalMethods.extraStrategies ++
      extraPlanningStrategies ++ (
      DataSourceV2Strategy ::
      FileSourceStrategy ::
      DataSourceStrategy(conf) ::
      SpecialLimits ::
      Aggregation ::
      JoinSelection ::
      InMemoryScans ::
      BasicOperators :: Nil)

```

#### FileSourceStrategy
扫描可以 partitioned 文件集合的计划策略。主要有几个阶段：
1) 需要估计的时候，可以分割文件
2) 基于投影裁剪元数据。除了在 top level 支持列裁剪，最好也能支持网状列裁剪(例如：parquet元数据)
3) 文件 reader 支持谓词下推和列裁剪
4) 文件 reader 支持 partition
5) 分割文件、构造 FileScanRDD
6) scan 之后增加 投影和筛选

```
 * A strategy for planning scans over collections of files that might be partitioned or bucketed
 * by user specified columns.

 * At a high level planning occurs in several phases:
 *  - Split filters by when they need to be evaluated.
 *  - Prune the schema of the data requested based on any projections present. Today this pruning
 *    is only done on top level columns, but formats should support pruning of nested columns as
 *    well.
 *  - Construct a reader function by passing filters and the schema into the FileFormat.
 *  - Using a partition pruning predicates, enumerate the list of files that should be read.
 *  - Split the files into tasks and construct a FileScanRDD.
 *  - Add any projection or filters that must be evaluated after the scan.

```

#### DataSourceStrategy
将 Catalyst [[Expression]] 翻译成 data source [[Filter]]

#### SpecialLimits
将 limit 转换为 TopK 操作。将 TopK 传播到所有 partition 。

#### JoinSelection
对 join 选择合适的 physical plan。
首先，使用 [ExtractEquiJoinKeys] 模式发现一些可以评估的谓词。如果有，则执行如下过程：
1) Broadcast hash join (BHJ):

BHJ 不支持 full outer join。除此之外，其他 join 都能支持。当 broadcast 数据量比较小的时候，BHJ 会很快。
但是，BHJ 是网络敏感型操作。而且，如果数据量比较大的话，有可能发生 OOM。

用户可以定义 broadcast hint(此时，不考虑数据量) 或者配置 AUTO_BROADCASTJOIN_THRESHOLD 以调整什么时候使用 BHJ。

即使两个表都满足 BHJ 条件，也只会选择其中一个进行 broadcast。

2) Shuffle hash join

一个 single partition 的平均数据量足够小，会使用 hash table。

3) Sort merge join
通常情况下Hash Join的效果都比排序合并连接要好，然而如果两表已经被排过序，在执行排序合并连接时不需要再排序了，这时Merge Join的性能会优于Hash Join。
Merge join的操作通常分三步：

step 1. 对连接的每个表做 table access full(全表扫描);
step 2. 对 table access full 的结果进行排序。
step 3. 进行 merge join 对排序结果进行合并。

通常情况下hash join的效果都比sort merge join要好，然而如果行源已经被排过序，在执行sort merge join时不需要再排序了，这时sort merge join的性能会优于hash join。

Spark 执行 SMJ 时，在 Shuffle 的 map 阶段做局部排序，在 Shuffle 的 reduce 阶段(by partition)首先合并、计算、最后归并排序。

4) BroadcastNestedLoopJoin (BNLJ): 适用任意场景
对于被连接的数据子集较小的情况，Nested Loop是个较好的选择。
Nested Loop就是扫描一个表（外表），每读到一条记录，就根据Join字段上的索引去另一张表（内表）里面查找，若Join字段上没有索引查询优化器一般就不会选择 Nested Loop。
在Nested Loop中，内表（一般是带索引的大表）被外表（也叫“驱动表”，一般为小表——不紧相对其它表为小表，而且记录数的绝对值也较小，不要求有索引）驱动，
外表返回的每一行都要在内表中检索找到与它匹配的行，因此整个查询返回的结果集不能太大（大于1 万不适合）。

5) Cartesian Product : 笛卡尔积


