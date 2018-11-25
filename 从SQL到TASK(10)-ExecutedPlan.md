#

## SparkPlan.execute
触发物理计划的执行，返回结果是 RDD[InternalRow] 。实际的 doExecute 由各个具体的 物理计划 Exec 来实现。
```
  /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to `doExecute` after
   * preparations.
   *
   * Concrete implementations of SparkPlan should override `doExecute`.
   */
  final def execute(): RDD[InternalRow] = executeQuery {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("A canonicalized plan is not supposed to be executed.")
    }
    doExecute()
  }

```

## BroadcastHashJoinExec.doExecute
会调用 BroadcastExchangeExec doExecuteBroadcast

### BroadcastExchangeExec doExecuteBroadcast
1) relationFuture : 启动一个Future(独立线程)，开始 broadcast
SparkContext.broadcast 
  -> BroadcastManager.newBroadcast
    -> TorrentBroadcastFactory.newBroadcast
      -> TorrentBroadcast.writeBlocks
        -> BlockManager.putSignle broadcastId
        -> BlockManager.putBytes  pieceId
        
2) 调用 doExecuteBroadcast 等待 broadcast 结束

### streamedPlan.execute
基于 partition 执行 join 操作 (即： Hash Join 的另一数据)。有几个步骤：
1) streamedPlan.execute(), 参与 join 另一方的执行
2) mapPartitions 在 join 另一方的所有 partition 上应用 join 操作
3) join(...) 执行 join 操作 (由一系列 map 组成)

mapPartitions 的过程由 map 组成，所以它是窄依赖

```
streamedPlan.execute().mapPartitions { streamedIter =>
      val hashed = broadcastRelation.value.asReadOnlyCopy()
      TaskContext.get().taskMetrics().incPeakExecutionMemory(hashed.estimatedSize)
      join(streamedIter, hashed, numOutputRows, avgHashProbe)
    }
```

## SortExec.doExecute
基于 partition 执行 sort. 有几个步骤：
1) create sorter
2) sort by sorter
3) 在 sort 的过程中，会有 spill ，更新 spill 统计
```
    child.execute().mapPartitionsInternal { iter =>
      val sorter = createSorter()

      val metrics = TaskContext.get().taskMetrics()
      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val spillSizeBefore = metrics.memoryBytesSpilled
      val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
      sortTime += sorter.getSortTimeNanos / 1000000
      peakMemory += sorter.getPeakMemoryUsage
      spillSize += metrics.memoryBytesSpilled - spillSizeBefore
      metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)

      sortedIterator
    }
```

## SortMergeJoinExec.doExecute
1) 构造 condition 判断函数，在 join 过程中决定是否匹配
2) 构造 SMJ(Sort Merge Join) Scanner, 包含：LeftKeyGenerator、RightKeyGenerator、keyOrdering(comparator)、leftIter、rightIter、inMemoryThreshold、spillThreshold
3) 可以看到， SMJscanner 包含了抽象排序器的所有必要内容
4) 构造 RowIterator， 使用 SMJscanner 、重载 advanceNext
5) 同样，这也是一个窄依赖

```
  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val spillThreshold = getSpillThreshold
    val inMemoryThreshold = getInMemoryThreshold
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val boundCondition: (InternalRow) => Boolean = {
        condition.map { cond =>
          newPredicate(cond, left.output ++ right.output).eval _
        }.getOrElse {
          (r: InternalRow) => true
        }
      }

      // An ordering that can be used to compare keys from both sides.
      val keyOrdering = newNaturalAscendingOrdering(leftKeys.map(_.dataType))
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)

      joinType match {
        case _: InnerLike =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] var currentRightMatches: ExternalAppendOnlyUnsafeRowArray = _
            private[this] var rightMatchesIterator: Iterator[UnsafeRow] = null
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold
            )
            private[this] val joinRow = new JoinedRow

            if (smjScanner.findNextInnerJoinRows()) {
              currentRightMatches = smjScanner.getBufferedMatches
              currentLeftRow = smjScanner.getStreamedRow
              rightMatchesIterator = currentRightMatches.generateIterator()
            }

            override def advanceNext(): Boolean = {
              while (rightMatchesIterator != null) {
                if (!rightMatchesIterator.hasNext) {
                  if (smjScanner.findNextInnerJoinRows()) {
                    currentRightMatches = smjScanner.getBufferedMatches
                    currentLeftRow = smjScanner.getStreamedRow
                    rightMatchesIterator = currentRightMatches.generateIterator()
                  } else {
                    currentRightMatches = null
                    currentLeftRow = null
                    rightMatchesIterator = null
                    return false
                  }
                }
                joinRow(currentLeftRow, rightMatchesIterator.next())
                if (boundCondition(joinRow)) {
                  numOutputRows += 1
                  return true
                }
              }
              false
            }

            override def getRow: InternalRow = resultProj(joinRow)
          }.toScala

```

## CollectLimitExec.doExecute
上面这些 Exec 都是窄依赖，limit 不同在于(有触发 Action 的operation)
1) 执行 child 物理计划，根据 partition 执行 take(limit)
2) take limit 会触发新的 job running
`val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, p)`
3) 构建 ShuffledRowRDD (初始化 ShuffleDependency)
4) ShuffledRowRDD 的计算过程会用到 BlockStoreShuffleReader.read，通过 BlockManager 从 executor 获取数据
5) 根据 shuffle rdd 的 partition 结果 ，执行 take(limit)，即实际调用 BlockManager 的 read 函数
```
val locallyLimited = child.execute().mapPartitionsInternal(_.take(limit))
    val shuffled = new ShuffledRowRDD(
      ShuffleExchangeExec.prepareShuffleDependency(
        locallyLimited, child.output, SinglePartition, serializer))
    shuffled.mapPartitionsInternal(_.take(limit))
```
