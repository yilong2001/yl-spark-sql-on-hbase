#

## QueryExecution
QueryExecution 是 Spark 执行 SQL查询的主要实现。定义了开放式插件接口，用户可自定义Rule和Strategy

### analyzed
```
  lazy val analyzed: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.analyzer.executeAndCheck(logical)
  }

```

(1) Hints 

支持通过 Hints 的方式，来完成 "BROADCAST", "BROADCASTJOIN", "MAPJOIN" 。
其他 Hints 将被删除。

可以自定义 Hints。

例如：
```
val qBroadcastLeft = """
  SELECT /*+ BROADCAST (lf) */ * FROM range(100) lf, range(1000) rt
  WHERE lf.id = rt.id
"""

val qBroadcastRight = """
 SELECT /*+ MAPJOIN (rt) */ * FROM range(100) lf, range(1000) rt
 WHERE lf.id = rt.id
"""
```

(2) Simple Sanity Check

删除 UnresolvedFunction , 如果这个 UnresolvedFunction 没有注册的话

(3) Substitution

(3.1) CTESubstitution, 把 CTE 替换为解析后的 逻辑计划

CTE的应用场景：

1） 使用 CTE 做递归查询

2） 作用类似于 view ，但无需事先保存元数据。

3） 从一个 scalar 子查询集合做 group by，或者用作无明确结果、依赖外部数据的函数

4） 使用相同的语句，多次重复访问结果表

(3.2) WindowsSubstitution, 

将未解析的窗口函数名称替换为具体的窗口属性值。

(3.3) 优化 Union ，如果只有一个Child、移除 Union

(3.4) 在 'order by' or 'group by' 中的序数以 UnresolvedOrdinal 表达式替换

(4) Resolve

(4.1) ResolveTableValuedFunctions

`resolves table-valued function`, 例如 ： range、end、start、step 等

(4.2) 使用具体的关系，替换 [UnresolvedRelation]s，例如 InMemoryRelation、TableScan 等

(4.3) 使用具体的属性，替换 [UnresolvedAttribute]，例如 select * from 中的 *

(4.*) 更多 Resolve 功能
```
      ResolveCreateNamedStruct ::
      ResolveDeserializer ::
      ResolveNewInstance ::
      ResolveUpCast ::
      ResolveGroupingAnalytics ::
      ResolvePivot ::
      ResolveOrdinalInOrderByAndGroupBy ::
      ResolveAggAliasInGroupBy ::
      ResolveMissingReferences ::
      ExtractGenerator ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ResolveSubquery ::
      ResolveSubqueryColumnAliases ::
      ResolveWindowOrder ::
      ResolveWindowFrame ::
      ResolveNaturalAndUsingJoin ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      TimeWindowing ::
      ResolveInlineTables(conf) ::
      ResolveTimeZone(conf) ::
      ResolvedUuidExpressions ::
```

(5) Post-Hoc Resolution

(5.1) PreprocessTableCreation

(5.2) PreprocessTableInsertion

(5.3) DataSourceAnalysis

(6) 更新 Outer Reference
例子如下：
```
 * For example (SQL):
 * {{{
 *   SELECT l.a FROM l GROUP BY 1 HAVING EXISTS (SELECT 1 FROM r WHERE r.d < min(l.b))
 * }}}
 * Plan before the rule.
 *    Project [a#226]
 *    +- Filter exists#245 [min(b#227)#249]
 *       :  +- Project [1 AS 1#247]
 *       :     +- Filter (d#238 < min(outer(b#227)))       <-----
 *       :        +- SubqueryAlias r
 *       :           +- Project [_1#234 AS c#237, _2#235 AS d#238]
 *       :              +- LocalRelation [_1#234, _2#235]
 *       +- Aggregate [a#226], [a#226, min(b#227) AS min(b#227)#249]
 *          +- SubqueryAlias l
 *             +- Project [_1#223 AS a#226, _2#224 AS b#227]
 *                +- LocalRelation [_1#223, _2#224]
 * Plan after the rule.
 *    Project [a#226]
 *    +- Filter exists#245 [min(b#227)#249]
 *       :  +- Project [1 AS 1#247]
 *       :     +- Filter (d#238 < outer(min(b#227)#249))   <-----
 *       :        +- SubqueryAlias r
 *       :           +- Project [_1#234 AS c#237, _2#235 AS d#238]
 *       :              +- LocalRelation [_1#234, _2#235]
 *       +- Aggregate [a#226], [a#226, min(b#227) AS min(b#227)#249]
 *          +- SubqueryAlias l
 *             +- Project [_1#223 AS a#226, _2#224 AS b#227]
 *                +- LocalRelation [_1#223, _2#224]
```

(7) FixNullability : 修复因 null field 的 NULL 值被优化为非 NULL 值的 bug



### 自定义 Rules 

Analyzer 提供了自定义接口，可以增加自定义 ：customResolutionRules、customPostHocResolutionRules、customCheckRules
```
  /**
   * Logical query plan analyzer for resolving unresolved attributes and relations.
   *
   * Note: this depends on the `conf` and `catalog` fields.
   */
  protected def analyzer: Analyzer = new Analyzer(catalog, conf) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        customResolutionRules

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      PreprocessTableCreation(session) +:
        PreprocessTableInsertion(conf) +:
        DataSourceAnalysis(conf) +:
        customPostHocResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
        PreReadCheck +:
        HiveOnlyCheck +:
        customCheckRules
  }

```

### 自定义 Rule/Strategy/CheckRule
(1) 自定义 Rule、Strategy
```
case class MyRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyCheckRule(spark: SparkSession) extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = { }
}

case class MySparkStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Seq.empty
}

```

(2) 自定义 Extensions
```
class MyExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPlannerStrategy(MySparkStrategy)
    e.injectResolutionRule(MyRule)
  }
}
```

(2) 配置 spark.sql.extensions
```
    val session = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions", classOf[MyExtensions].getCanonicalName)
      .getOrCreate()
```

### 缓存替换
参考：07-Cachemanager1

```
  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    sparkSession.sharedState.cacheManager.useCachedData(analyzed)
  }

```


