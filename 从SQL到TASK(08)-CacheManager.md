#

## org.apache.spark.sql.executionCacheManager
CacheManager 将 LogicPlan 中已处理过的 Plan 替换为 InMemoryRelation。这样，可以重复利用已经计算得到的 RDD。

### Cache
1、缓存 Dataset 逻辑计划生成的 Data

2、默认的缓存级别是 MEMORY_AND_DISK，确保在 Task Error 的时候，能给从 Disk 恢复数据
```
  /**
   * Caches the data produced by the logical representation of the given [[Dataset]].
   * Unlike `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because
   * recomputing the in-memory columnar representation of the underlying table is expensive.
   */
  def cacheQuery(
      query: Dataset[_],
      tableName: Option[String] = None,
      storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = writeLock {
    val planToCache = query.logicalPlan
    if (lookupCachedData(planToCache).nonEmpty) {
      logWarning("Asked to cache already cached data.")
    } else {
      val sparkSession = query.sparkSession
      val inMemoryRelation = InMemoryRelation(
        sparkSession.sessionState.conf.useCompression,
        sparkSession.sessionState.conf.columnBatchSize, storageLevel,
        sparkSession.sessionState.executePlan(planToCache).executedPlan,
        tableName,
        planToCache.stats)
      cachedData.add(CachedData(planToCache, inMemoryRelation))
    }
  }
```

###  触发 Cache : RDD.persist()
调用 persist 会 Cache DataSet 逻辑计划生成的 Data，默认级别 MEMORY_AND_DISK

```
  def persist(): this.type = {
    sparkSession.sharedState.cacheManager.cacheQuery(this)
    this
  }
  
  def persist(newLevel: StorageLevel): this.type = {
    sparkSession.sharedState.cacheManager.cacheQuery(this, None, newLevel)
    this
  }
```

### 触发 Cache : SQLContext.cacheTale()
调用 cacheTable 会 Cache Table 对应 DataFrame 的 Data, 默认级别 MEMORY_AND_DISK
```
  def cacheTable(tableName: String): Unit = {
    sparkSession.catalog.cacheTable(tableName)
  }
```

从触发操作来看，Cache 动作由应用控制。
 
### Re Cache
如果 DataSet(RDD) 有更新动作，例如：Insert ，会触发 Cache 更新(前提是：已经有 Cache)

判断一个逻辑计划是否已经有缓存
```
  def recacheByPlan(spark: SparkSession, plan: LogicalPlan): Unit = writeLock {
    recacheByCondition(spark, _.find(_.sameResult(plan)).isDefined)
  }
```

如果有缓存的话，删除原有的缓存，用一个新的 InMemoryRelation 代替
```
  private def recacheByCondition(spark: SparkSession, condition: LogicalPlan => Boolean): Unit = {
    val it = cachedData.iterator()
    val needToRecache = scala.collection.mutable.ArrayBuffer.empty[CachedData]
    while (it.hasNext) {
      val cd = it.next()
      if (condition(cd.plan)) {
        cd.cachedRepresentation.cachedColumnBuffers.unpersist()
        // Remove the cache entry before we create a new one, so that we can have a different
        // physical plan.
        it.remove()
        val newCache = InMemoryRelation(
          useCompression = cd.cachedRepresentation.useCompression,
          batchSize = cd.cachedRepresentation.batchSize,
          storageLevel = cd.cachedRepresentation.storageLevel,
          child = spark.sessionState.executePlan(cd.plan).executedPlan,
          tableName = cd.cachedRepresentation.tableName,
          statsOfPlanToCache = cd.plan.stats)
        needToRecache += cd.copy(cachedRepresentation = newCache)
      }
    }

    needToRecache.foreach(cachedData.add)
  }
```

### Query Cache
查询的过程: 即使用缓存的 InMemoryRelation 代替 LogicalPlan . 

这个 LogicalPlan 被替换完成之后的节点是一个 LeafNode。也就是说，所有这个 LogicalPlan 包含的其他操作(filter\join\...)都统一被替换了。

```
  /** Replaces segments of the given logical plan with cached versions where possible. */
  def useCachedData(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan transformDown {
      // Do not lookup the cache by hint node. Hint node is special, we should ignore it when
      // canonicalizing plans, so that plans which are same except hint can hit the same cache.
      // However, we also want to keep the hint info after cache lookup. Here we skip the hint
      // node, so that the returned caching plan won't replace the hint node and drop the hint info
      // from the original plan.
      case hint: ResolvedHint => hint

      case currentFragment =>
        lookupCachedData(currentFragment)
          .map(_.cachedRepresentation.withOutput(currentFragment.output))
          .getOrElse(currentFragment)
    }

    newPlan transformAllExpressions {
      case s: SubqueryExpression => s.withNewPlan(useCachedData(s.plan))
    }
  }
```



