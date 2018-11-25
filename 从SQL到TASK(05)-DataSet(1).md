#

## object Dataset 从 LogicalPlan 到 DataFrame
有了逻辑计划，就可以使用 Dataset.ofRows 生成 DataFrame
(1) Create QueryExecution，即 `new QueryExecution(sparkSession, logicalPlan)`
(2) new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
(3) RowEncoder 由 QueryExecution 的 schema 所组成，用来完成 序列化

schema 由 Atributes 转换而成，每一层 LogicalPlan 的 schema 可能都不相同

```
private[sql] object Dataset {
  def apply[T: Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[T] = {
    val dataset = new Dataset(sparkSession, logicalPlan, implicitly[Encoder[T]])
    // Eagerly bind the encoder so we verify that the encoder matches the underlying
    // schema. The user will get an error if this is not the case.
    dataset.deserializer
    dataset
  }

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }
}
```
