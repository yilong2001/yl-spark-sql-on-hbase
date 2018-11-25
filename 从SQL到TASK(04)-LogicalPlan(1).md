#

## LogicalPlan
LogicalPlan 是 QueryPlan 的子类。
```
abstract class LogicalPlan
  extends QueryPlan[LogicalPlan]
  with LogicalPlanStats
  with QueryPlanConstraints
  with Logging {

```

QueryPlan 是 TreeNode 的子类。所以，LogicalPlan 是一棵树形结构，其树形结构的节点也是 LogicalPlan。
```
abstract class QueryPlan[PlanType <: QueryPlan[PlanType]] extends TreeNode[PlanType] {
```

TreeNode 由 Seq[BaseType] 组成 ，BaseType 在 LogicalPlan 中就是 LogicalPlan 自身。
```
abstract class TreeNode[BaseType <: TreeNode[BaseType]] extends Product {
// scalastyle:on
  self: BaseType =>

  val origin: Origin = CurrentOrigin.get

  /**
   * Returns a Seq of the children of this node.
   * Children should not change. Immutability required for containsChild optimization
   */
  def children: Seq[BaseType]

  lazy val containsChild: Set[TreeNode[_]] = children.toSet

```

## 实例
```
"select mobile as mb, sid as id, mobile*2 multi2mobile, count(1) times from (select * from temp_mobile
)a where pfrom_id=0.0 group by mobile, sid,  mobile*2"
```
生成的逻辑计划为：
(1) 根结点为 Aggregate 节点：
其中，聚合字段：['mobile,'sid,('mobile * 2) AS c2#27], 
输出字段：['mobile AS mb#23,'sid AS id#24,('mobile * 2) AS multi2mobile#25,COUNT(1) AS times#26L]  
(2) 子节点为 Filter 节点，条件为 ('pfrom_id = 0.0)  
(3) 紧接着是子查询 ( subquery ), 实际上是一个 Project 参数(column)是 *，即所有列
(4) 叶子节点是 UnresolvedRelation：也就是数据源 temp_mobile

```
!Aggregate ['mobile,'sid,('mobile * 2) AS c2#27], ['mobile AS mb#23,'sid AS id#24,('mobile * 2) AS multi2mobile#25,COUNT(1) AS times#26L]   
! Filter ('pfrom_id = 0.0)                   
   Subquery a                                       
!   Project [*]                                   
!    UnresolvedRelation None, temp_mobile, None
```
