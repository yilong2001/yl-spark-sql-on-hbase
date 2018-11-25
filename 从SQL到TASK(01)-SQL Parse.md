#

## createTempView
1、使用 Spark SQL 之前，首先需要有 table（使用 hive context，则直接读取 hive table）

2、如果数据源是 file(local or hdfs)，需要使用 DataFrameReader 读取文件，生成 DataFrame，然后 createTempView

## 执行 SQL Query 并输出到 HDFS 文件
```
例如 SQL 
sparkSession.sqlContext.sql("SELECT COUNT(intField), stringField FROM normal_orc_source GROUP BY stringField").write.parquet("path")
```

## SQLContext.sql(...)
直接调用了 SparkSession 的 sql 函数, 返回值为 DataFrame。
也就是说，sql 返回的 DataFrame 还可以使用 spark core 继续转换。
```
  def sql(sqlText: String): DataFrame = sparkSession.sql(sqlText)
```

## SparkSession.sql(...)
1、首先对 SQL 语句进行 分析处理 `sessionState.sqlParser.parsePlan(sqlText)` ，生成逻辑计划
2、生成一个新的 Dataset
```
  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`.
   * The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @since 2.0.0
   */
  def sql(sqlText: String): DataFrame = {
    Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
  }
```

## sessionState.sqlParser.parsePlan(sqlText): AbstractSqlParser.parsePlan
主要是两部分：语法解析、生成逻辑计划
```
  /** Creates LogicalPlan for a given SQL string. */
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    astBuilder.visitSingleStatement(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ =>
        val position = Origin(None, None)
        throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
    }
  }
```
### 语法解析
1、词法解析器SqlBaseLexer和语法解析器SqlBaseParser
SqlBaseLexer和SqlBaseParser均是使用ANTLR 4自动生成的Java类。
这里，采用这两个解析器将 SQL 语句解析成了 ANTLR 4 的语法树结构 ParseTree
```
  protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    logDebug(s"Parsing command: $command")

    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position)
    }
  }
```
### 生成逻辑计划 AstBuilder (SqlBaseBaseVisitor)
2、使用 AstBuilder（AstBuilder.scala）将 ANTLR 4 语法树结构转换成 catalyst 表达式，即 logical plan
```
  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }
```

```
{ parser =>
    astBuilder.visitSingleStatement(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ =>
        val position = Origin(None, None)
        throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
  }
}
```

### SQL 解析原理
ANTLR 是一种 LL 类型的 文法解析器。
关于 LL 文法的更多内容，参考 "编译文法"。

### 自定义 parser
(1) 自定义 Parser
```
case class MyParser(spark: SparkSession, delegate: ParserInterface) extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan =
    delegate.parsePlan(sqlText)

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)
}
```

(2) 自定义 MyExtensions
```
class MyExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
      e.injectParser((_, _) => CatalystSqlParser)
      e.injectParser(MyParser)
      e.injectParser(MyParser)
  }
}

```

(3) 配置 spark.sql.extensions
```
    val session = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions", classOf[MyExtensions].getCanonicalName)
      .getOrCreate()

```

