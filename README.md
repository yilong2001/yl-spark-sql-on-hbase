# yl-sqlonhbase
一个 Spark SQL DataSource on HBase 的实现:
## 支持 Spark SQL On Hbase ，能够与其他 DataSource 执行 join 
## 在HBase scan阶段完成列裁剪、谓词下推
## 在Saprk实现了 RDD，在 HBase 实现了自定义 Filter 和 Coprocessor

## 1、HBaseRelationProvicer
实现 spark sql source 的核心是实现一个 RelationProvider，继承 RelationProvider 实现 createRelation 接口。

## 2、HBaseRelation
通过 Provider 创建 HBaseRelation， HBaseRelation 的核心是实现 buildScan 接口，创建 RDD (HBaseRDD)。
一个 RDD 实际上是一个 iterator<InternalRow> ，即迭代器。在触发计算的时候(sql 的 collection 操作)，RDD 才会拉取实际数据。

## 3、序列化问题 
HBaseRDD 在 runjob 阶段，作为 task 的一部分提交给 executor ，如果 RDD 依赖对象不能序列化，会导致 task submit 失败。

## 4、transient 
transient 对象不会序列化，RDD 作为任务的一部分被序列化的时候，如果有 transient 声明的字段，那么有可能值=null

## 5、hbase filter
在自定义 filter 作为 scanner 的一部分，在 RS 端运行过程中会进行匹配，首先是 rowkey 匹配、其次是每个 cell 匹配。
在 rowkey 匹配时，每次执行前，需要 reset filter，否则可能结果全部为 true

## 6、自定义 Strategy

## 7、自定义 hbase coprocessor

## 8、重定义 hbaserelation  sizeInBytes 接口

## 交流
wx: yilong2001
