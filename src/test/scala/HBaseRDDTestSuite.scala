import java.util
import java.util.function.Consumer

import com.example.spark.demo.impl.transfer.{ConcatTransfer, FieldValuePair, RowKeyTransfer, ValueTransfer}
import com.example.spark.sql.execution.hbase.HBaseOptions.{HBASE_TABLE_FAMILY_NAME, HBASE_TABLE_NAME, HBASE_TABLE_ROWKEY_TRANSFER, HBASE_TABLE_VALUE_CLASS_NAME}
import com.example.spark.sql.execution.hbase.{HBaseOptions, HBaseRDD, HBaseRelationProvider}
import com.example.spark.sql.execution.optimizer.{HBaseStatisOptimizationRule, HBaseStrategy}
import com.example.spark.sql.util.Serde
import org.apache.hadoop.hbase.filter
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.sql.sources.{And, EqualTo, Filter, Or}
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Created by yilong on 2018/6/13.
  */
object HBaseRDDTestSuite {
  val userAvsc = "{\"namespace\": \"com.example.spark.demo.generated.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"User\",\n" + " \"fields\": [\n" + "     {\"name\": \"name\", \"type\": \"string\"},\n" + "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" + "     {\"name\": \"arr\", \"type\": {\"type\": \"array\", \"items\": \"string\"}} " + "]}"

  val tab2Avsc = "{\n  \"type\" : \"record\",\n  \"name\" : \"TAB2\"," + "\n  \"namespace\" : \"MYSCH2\",\n  \"fields\" : [ {\n    \"name\" : \"table\"," + "\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_type\"," + "\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_ts\"," + "\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"current_ts\"," + "\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"pos\"," + "\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"primary_keys\"," + "\n    \"type\" : {\n      \"type\" : \"array\"," + "\n      \"items\" : \"string\"\n    }\n  }, " + "{\n    \"name\" : \"tokens\",\n    \"type\" : {\n      \"type\" : \"map\"," + "\n      \"values\" : \"string\"\n    },\n    \"default\" : { }\n  }, " + "{\n    \"name\" : \"before\",\n    \"type\" : [ \"null\", " + "{\n      \"type\" : \"record\",\n      \"name\" : \"columns\"," + "\n      \"fields\" : [ {\n        \"name\" : \"ID\"," + "\n        \"type\" : [ \"null\", \"string\" ]," + "\n        \"default\" : null\n      }, {\n        \"name\" : \"ID_isMissing\"," + "\n        \"type\" : \"boolean\"\n      }, {\n        \"name\" : \"NAME\"," + "\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, " + "{\n        \"name\" : \"NAME_isMissing\",\n        \"type\" : \"boolean\"\n      } ]\n    } ],\n    \"default\" : null\n  }, {\n    \"name\" : \"after\",\n    \"type\" : [ \"null\", \"columns\" ],\n    \"default\" : null\n  } ]\n}"


  def buildRowkeyTransfer() : RowKeyTransfer = {
    val vf1 = new ValueTransfer("A")
    val vf2 = new ValueTransfer("B")
    val vf3 = new ValueTransfer("C")
    val vf4 = new ValueTransfer("D")

    var z:Array[RowKeyTransfer] = new Array[RowKeyTransfer](4)
    z(0) = vf1
    z(1) = vf2
    z(2) = vf3
    z(3) = vf4

    val vf = new ConcatTransfer(z)

    vf
  }

  def testRowkeyTransfer_1(rkt : RowKeyTransfer, filters : Array[Filter]) : Unit = {
    val (hbasefilterlist, _) = HBaseRDD.buildRowkeyAndValueFilter(rkt, filters)

    hbasefilterlist.asInstanceOf[filter.FilterList].getFilters.foreach(f1 => {
      if (f1.isInstanceOf[filter.FilterList]) {
        f1.asInstanceOf[filter.FilterList].getFilters.foreach(f2 => {
          println(new String(f2.asInstanceOf[filter.RowFilter].getComparator.getValue))
          println((f2.asInstanceOf[filter.RowFilter].getOperator))
        })
      } else if (f1.isInstanceOf[filter.RowFilter]) {
        println(new String(f1.asInstanceOf[filter.RowFilter].getComparator.getValue))
        println((f1.asInstanceOf[filter.RowFilter].getOperator))
      } else {
        println("error")
      }
    })
  }

  def testRowkeyTransfer(rkt : RowKeyTransfer, tt : util.List[FieldValuePair]) : Unit = {
    val hbasefilterlist = HBaseRDD.compileFieldValuePairGroupForRowFilter(rkt, tt)

    hbasefilterlist.asInstanceOf[org.apache.hadoop.hbase.filter.FilterList].getFilters.foreach(f1 => {
      println(new String(f1.asInstanceOf[filter.RowFilter].getComparator.getValue))
      println((f1.asInstanceOf[filter.RowFilter].getOperator))
    })
  }

  def testValueCondition(): Unit = {
    val f = And(Or(EqualTo("A", 100), EqualTo("C", 200)) , Or(EqualTo("B", 300), EqualTo("D", 400)))

    val arra = new Array[Filter](1)
    val arr2 = Array[Filter](f)
    val filterListsTmp = HBaseRDD.travelSqlQueryFilters(arr2)//travelSqlQueryFilters(arr2)
    val filterListsLast = new util.ArrayList[util.List[Filter]]()

    HBaseRDD.compileSqlQueryFilters(filterListsTmp, filterListsLast)

    val tkf = buildRowkeyTransfer()
    val fvls = HBaseRDD.transferSqlQueryFilters(filterListsLast)

    val vcs = HBaseRDD.compileFieldValuePairGroupsForValueCondition(fvls)

    class TestVC(val A : Int, val B : Int, val C : Int, val D : Int) {
      def getA():Int = A
      def getB():Int = B
      def getC():Int = C
      def getD():Int = D
    }

    val tvc1 = new TestVC(100,201,301,401)
    assert(!vcs.isMatched(tvc1))

    val tvc2 = new TestVC(100,201,301,400)
    assert(vcs.isMatched(tvc2))

    val tvc3 = new TestVC(101,300,200,401)
    assert(vcs.isMatched(tvc3))

    val tvc4 = new TestVC(101,301,200,401)
    assert(!vcs.isMatched(tvc4))

    assert(tkf.transfer(tvc1).equals("100_201_301_401"))
  }

  def testQueryFilterBuilder() : Unit = {
    //val f = And(Or(EqualTo("A", 100), EqualTo("B", 200)) , Or(EqualTo("C", 300), EqualTo("D", 400)))
    val f = And(Or(EqualTo("A", 100), EqualTo("C", 200)) , Or(EqualTo("B", 300), EqualTo("D", 400)))

    val arra = new Array[Filter](1)
    val arr2 = Array[Filter](f)
    val filterListsTmp = HBaseRDD.travelSqlQueryFilters(arr2)
    val filterListsLast = new util.ArrayList[util.List[Filter]]()

    HBaseRDD.compileSqlQueryFilters(filterListsTmp, filterListsLast)

    val f1 = new Array[Filter](2)
    f1(0) = EqualTo("A", 100)
    f1(1) = EqualTo("B", 300)

    val f2 = new Array[Filter](2)
    f2(0) = EqualTo("C", 200)
    f2(1) = EqualTo("B", 300)

    val f3 = new Array[Filter](2)
    f3(0) = EqualTo("A", 100)
    f3(1) = EqualTo("D", 400)

    val f4 = new Array[Filter](2)
    f4(0) = EqualTo("C", 200)
    f4(1) = EqualTo("B", 300)

    var cout = 0
    filterListsLast.forEach(new Consumer[util.List[Filter]] {
      override def accept(tt: util.List[Filter]): Unit = {
        var tmpct = 0
        f1.foreach(tmp => {
          tt.forEach(new Consumer[Filter] {
            override def accept(t: Filter): Unit = if (tmp.equals(t)) tmpct=tmpct+1
          })
        })

        if (tmpct == f1.size) cout = cout + 1

        tmpct = 0
        f2.foreach(tmp => {
          tt.forEach(new Consumer[Filter] {
            override def accept(t: Filter): Unit = if (tmp.equals(t)) tmpct=tmpct+1
          })
        })

        if (tmpct == f2.size) cout = cout + 1

        tmpct = 0
        f3.foreach(tmp => {
          tt.forEach(new Consumer[Filter] {
            override def accept(t: Filter): Unit = if (tmp.equals(t)) tmpct=tmpct+1
          })
        })

        if (tmpct == f3.size) cout = cout + 1

        tmpct = 0
        f4.foreach(tmp => {
          tt.forEach(new Consumer[Filter] {
            override def accept(t: Filter): Unit = if (tmp.equals(t)) tmpct=tmpct+1
          })
        })

        if (tmpct == f4.size) cout = cout + 1
      }
    })

    println(cout)
    assert(cout == 4)
  }

  def buildSparkFilters() : Array[Filter] = {
    val f = And(Or(EqualTo("A", 100), EqualTo("C", 200)) , Or(EqualTo("B", 300), EqualTo("D", 400)))
    val arr2 = Array[Filter](f)
    arr2
  }

  def testFilterTransfers() : Unit = {
    //val f = And(Or(EqualTo("A", 100), EqualTo("B", 200)) , Or(EqualTo("C", 300), EqualTo("D", 400)))
    val f = And(Or(EqualTo("A", 100), EqualTo("C", 200)) , Or(EqualTo("B", 300), EqualTo("D", 400)))

    val arra = new Array[Filter](1)
    val arr2 = Array[Filter](f)
    val filterListsTmp = HBaseRDD.travelSqlQueryFilters(arr2)
    val filterListsLast = new util.ArrayList[util.List[Filter]]()

    HBaseRDD.compileSqlQueryFilters(filterListsTmp, filterListsLast)

    filterListsLast.foreach(filterlist => {
      filterlist.foreach(filter => {
        println(filter)
      })
    })

    val tkf = buildRowkeyTransfer()
    val fvls = HBaseRDD.transferSqlQueryFilters(filterListsLast)
    fvls.foreach(fvplist => {
      testRowkeyTransfer(tkf, fvplist)
    })

  }

  def getAvroT1DFReader(sparkSession : SparkSession) : DataFrameReader = {
    val dfr = sparkSession.read.format(classOf[HBaseRelationProvider].getName)
      .option(HBaseOptions.HBASE_TABLE_NAME, "avro_t1")
      .option(HBaseOptions.HBASE_TABLE_FAMILY_NAME, "f")
      .option(HBaseOptions.HBASE_TABLE_ROWKEY_TRANSFER, Serde.serialize(new ValueTransfer("table")))
      .option(HBaseOptions.HBASE_TABLE_AVSC, tab2Avsc)

    dfr
  }

  def getAvroT2DFReader(sparkSession : SparkSession) : DataFrameReader = {
    val dfr = sparkSession.read.format(classOf[HBaseRelationProvider].getName)
      .option(HBaseOptions.HBASE_TABLE_NAME, "avro_t2")
      .option(HBaseOptions.HBASE_TABLE_FAMILY_NAME, "f")
      .option(HBaseOptions.HBASE_TABLE_ROWKEY_TRANSFER, Serde.serialize(new ValueTransfer("table")))
      .option(HBaseOptions.HBASE_TABLE_AVSC, userAvsc)

    dfr
  }

  def testBasicHbaseScanRDD_1() : Unit = {
    val filter = Or(EqualTo("table", "table1"), EqualTo("table", "table0"))
    val sparkSession = createSparkSession()

    val dfr = getAvroT1DFReader(sparkSession)

    val df = dfr.load()
    df.show()

    //df.cache()
    df.toDF().createTempView("avro1")
    val subdf = sparkSession.sqlContext.sql("select * from avro1 where table='table1'")

    println(subdf.queryExecution.logical)
    println(subdf.queryExecution.analyzed)
    println(subdf.queryExecution.optimizedPlan)
    println(subdf.queryExecution.executedPlan)
    println(subdf.queryExecution.executedPlan)
    println(subdf.queryExecution.sparkPlan)

    subdf.show()
  }

  def testBasicHbaseScanRDD_2() : Unit = {
    import org.apache.spark.sql

    val filter = Or(EqualTo("table", "table1"), EqualTo("table", "table0"))
    val sparkSession = createSparkSession()

    sparkSession.experimental.extraStrategies = Seq(HBaseStrategy)
    sparkSession.experimental.extraOptimizations = Seq(HBaseStatisOptimizationRule)

    val dfr2 = getAvroT2DFReader(sparkSession)
    val df2 = dfr2.load()
    df2.toDF().createTempView("avro2")

    val dfr1 = getAvroT1DFReader(sparkSession)
    val df1 = dfr1.load()
    df1.toDF().createTempView("avro1")

    val sqljoin = "select a2.*, a1.* from avro2 a2 inner join avro1 a1 on a1.table = a2.name where a1.table='table1'"
    val subdf = sparkSession.sqlContext.sql(sqljoin)

    println(subdf.queryExecution.logical)
    println(subdf.queryExecution.analyzed)
    //subdf.queryExecution.analyzed
    println(subdf.queryExecution.optimizedPlan)
    println(subdf.queryExecution.executedPlan)
    println(subdf.queryExecution.sparkPlan)

    subdf.write.csv()
    //subdf.show()
  }

  def testHBaseScanRddPlan() : Unit = {
    val sparkSession = createSparkSession()
    val dfr = getAvroT1DFReader(sparkSession)
    println(dfr.isInstanceOf[org.apache.spark.sql.sources.v2.reader.SupportsReportPartitioning]);
    println(dfr.isInstanceOf[org.apache.spark.sql.sources.v2.reader.SupportsScanUnsafeRow]);
    println(dfr.isInstanceOf[org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch]);
    println(dfr.isInstanceOf[org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader]);
  }

  def createSparkSession() = {
    val conf = new SparkConf().setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(conf)

    var ss : SparkSession = null
    try {
      ss = sparkSession.getOrCreate()
    } catch {
      case e : Exception => e.printStackTrace()
    }

    ss
  }


  def main(args: Array[String]): Unit = {
    //testValueCondition()
    //testRowkeyTransfer_1(buildRowkeyTransfer(), buildSparkFilters())
    //testFilterTransfers()
    //testBasicHbaseScanRDD_1()
    testBasicHbaseScanRDD_2()
    //testHBaseScanRddPlan()
  }
}
