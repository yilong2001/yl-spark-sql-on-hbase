#

## SparkPlan -- prepareForExecution --> SparkPlan
SparkPlan(Physical plan) 再经过一系列规则转换，为 doExecute 作准备。
主要有：
1) 对于子查询表达式，替换为 SparkPlan (物理计划)
2) 校验分区一致性(特别是对 join)
3) 代码生成
4) 重用 exchange
5) 重用子查询

## 校验分区一致性 (EnsureRequirements)
判断数据的 Distribution 与 Partition 是否一致

### Distribution
```
 * Specifies how tuples that share common expressions will be distributed when a query is executed
 * in parallel on many machines.  Distribution can be used to refer to two distinct physical
 * properties:
 *  - Inter-node partitioning of data: In this case the distribution describes how tuples are
 *    partitioned across physical machines in a cluster.  Knowing this property allows some
 *    operators (e.g., Aggregate) to perform partition local operations instead of global ones.
 *  - Intra-partition ordering of data: In this case the distribution describes guarantees made
 *    about how tuples are distributed within a single partition.
```
当一个查询在多个节点上并行运行时，定义  tuples 是如何分布的。数据分布用于标示两种不同的物理属性：
1) Inter-node partitioning of data
描述 tuples 是以什么方式、在一个集群的物理机器上 分布(partitioned)。基于这个属性，一些操作(例如：aggregate)能在本地完成，而不需全局执行。

2) Intra-partition ordering of data
描述 tuples 在一个 partition 内部是如何分布的，例如：是否有序等。

### Distribution 分类
1) UnspecifiedDistribution
对数据在集群上的分布不做任何保证。

2) AllTuples
只有一个 partition ，所有数据都在同一个位置

3) ClusteredDistribution
具有相同 key (分区字段的值,可能是多个分区字段、或者一个tuple的值) 的数据，位于同一个 partition，或者只有一个 partition 
 
4) HashClusteredDistribution
tuple 有相同 hash value (hash 函数由 HashPartitioning.partitionIdExpression 定义)的数据，位于同一个 partition。
比 ClusteredDistribution 的要求更严格。目前实现是一样的。

5) OrderedDistribution
ClusteredDistribution (根据排序表达式) + Order (根据排序表达式)

6) BroadcastDistribution
tuples 会广播到所有节点

### Partition
```
 * Describes how an operator's output is split across partitions. It has 2 major properties:
 *   1. number of partitions.
 *   2. if it can satisfy a given distribution.
```
描述 operator 的输出是如何在 partitions 中被分割的。有两个主要属性：分区数、是否能满足一个给定的 distribution

### Partition 分类
1) UnknownPartitioning : 满足 UnspecifiedDistribution，如果 分区数=1，满足 AllTuples

2) RoundRobinPartitioning : 满足 UnspecifiedDistribution，如果 分区数=1，满足 AllTuples
```
 * Represents a partitioning where rows are distributed evenly across output partitions
 * by starting from a random target partition number and distributing rows in a round-robin
 * fashion. This partitioning is used when implementing the DataFrame.repartition() operator.
```

3) SinglePartition : 不满足 BroadcastDistribution ，其他都满足

4) HashPartitioning : 如果表达式(hash function)相同，满足 HashClusteredDistribution & ClusteredDistribution

5) RangePartitioning : 如果排序表达式与 OrderedDistribution 一致，则满足；如果表达式包含 ClusteredDistribution 的表达式，则满足；

``` * Represents a partitioning where rows are split across partitions based on some total ordering of
    * the expressions specified in `ordering`.  When data is partitioned in this manner the following
    * two conditions are guaranteed to hold:
    *  - All row where the expressions in `ordering` evaluate to the same values will be in the same
    *    partition.
    *  - Each partition will have a `min` and `max` row, relative to the given ordering.  All rows
    *    that are in between `min` and `max` in this `ordering` will reside in this partition.
    *
    * This class extends expression primarily so that transformations over expression will descend
    * into its child.

```

6) PartitioningCollection
```
 * A collection of [[Partitioning]]s that can be used to describe the partitioning
 * scheme of the output of a physical operator. It is usually used for an operator
 * that has multiple children. In this case, a [[Partitioning]] in this collection
 * describes how this operator's output is partitioned based on expressions from
 * a child. For example, for a Join operator on two tables `A` and `B`
 * with a join condition `A.key1 = B.key2`, assuming we use HashPartitioning schema,
 * there are two [[Partitioning]]s can be used to describe how the output of
 * this Join operator is partitioned, which are `HashPartitioning(A.key1)` and
 * `HashPartitioning(B.key2)`. It is also worth noting that `partitionings`
 * in this collection do not need to be equivalent, which is useful for
 * Outer Join operators.
```

7) BroadcastPartitioning
```
 * Represents a partitioning where rows are collected, transformed and broadcasted to each
 * node in the cluster.
```

## exchange
```
* Performs a shuffle that will result in the desired `newPartitioning`.  Optionally sorts each
* resulting partition based on expressions from the partition key.  It is invalid to construct an
* exchange operator with a `newOrdering` that cannot be calculated using the partitioning key.
```
在 EnsureRequirements 阶段，如果发现 数据的 Distribution 与 Partition 不匹配(例如，参与join的两个子节点)。
就会在物理计划中，增加一个 exchange 阶段，确保 exchange 之后的 Distribution 与 Partition 一致。

主要有两种原因：
1) child物理计划的输出数据分布不satisfies当前物理计划的数据分布要求。比如说child物理计划的数据输出分布是UnspecifiedDistribution，而当前物理计划的数据分布要求是ClusteredDistribution
2) 对于包含2个Child物理计划的情况，2个Child物理计划的输出数据有可能不compatile。因为涉及到两个Child物理计划采用相同的Shuffle运算方法才能够保证在本物理计划执行的时候一个分区的数据在一个节点，所以2个Child物理计划的输出数据必须采用compatile的分区输出算法。如果不compatile需要创建Exchange替换掉Child物理计划。

exchange 会执行 shuffle 操作。

### exchange 类型
1) BroadcastExchangeExec 
```
BroadcastExchangeExec is a Exchange unary physical operator to collect and broadcast rows of a child relation (to worker nodes).
BroadcastExchangeExec is created exclusively when EnsureRequirements physical query plan optimization 
ensures BroadcastDistribution of the input data of a physical operator 
(that can really be either BroadcastHashJoinExec or BroadcastNestedLoopJoinExec operators).
```

2) ShuffleExchangeExec
```
ShuffleExchangeExec is a Exchange unary physical operator to perform a shuffle.
ShuffleExchangeExec corresponds to Repartition (with shuffle enabled) and RepartitionByExpression 
logical operators (as resolved in BasicOperators execution planning strategy).
```

### reuse exchange
对于 SparkPlan 中相同的 exchange ，使用 ReusedExchangeExec 代替。 ReusedExchangeExec 在初次执行时，会 cache exchage 后的 RDD，再次使用时，优先使用 cached RDD。

