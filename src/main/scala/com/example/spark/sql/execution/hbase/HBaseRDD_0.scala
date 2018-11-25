package com.example.spark.sql.execution.hbase

import java.io.IOException
import java.nio.charset.Charset
import java.util
import java.util.Map
import java.util.function.{BiConsumer, Consumer}

import com.example.spark.demo.impl.cmp._
import com.example.spark.demo.impl.transfer.FieldValuePair.FieldCompareType
import com.example.spark.demo.impl.transfer.RowKeyTransfer.RowkeyFieldSortType
import com.example.spark.demo.impl.transfer.{ConcatTransfer, FieldValuePair, RowKeyTransfer, ValueTransfer}
import com.example.spark.hbase.filter.{SingleAvroFilter, SingleThriftFilter}
import com.example.spark.sql.execution.avro.AvroSchema
import com.example.spark.sql.util.{CompletionIterator, HBaseUtils, ThriftSerde}
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, TableName, filter}
import org.apache.hadoop.hbase.filter.{BinaryComparator, BinaryPrefixComparator, CompareFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.thrift.TFieldIdEnum
import org.apache.thrift.meta_data.{FieldMetaData, ListMetaData, StructMetaData}

import scala.reflect.internal.util.ScalaClassLoader
import scala.util.control.NonFatal
import scala.util.control._
import scala.collection.JavaConverters._

/**
  * Created by yilong on 2018/6/7.
  *
  */
//TODO: foreach 使用 immutuable 优化
case class HBasePartition_0(startRowKey: String, endRowKey: String, idx: Int) extends Partition {
  override def index: Int = idx
}

object HBaseRDD_0 extends Logging {

  def isEqualFilter(f : Filter) : Boolean = {
    f match {
      case EqualTo(attr, value) => true
      case _ => false
    }
  }

  def isGreaterFilter(f : Filter) : Boolean = {
    f match {
      case GreaterThan(attr, value) => true
      case GreaterThanOrEqual(attr, value) => true
      case _ => false
    }
  }

  def isLessFilter(f : Filter) : Boolean = {
    f match {
      case LessThan(attr, value) => true
      case LessThanOrEqual(attr, value) => true
      case _ => false
    }
  }

  def isComposeFilter(f : Filter) : Boolean = {
    f match {
      case Or(f1, f2) => true
      case And(f1, f2) => true
      case _ => false
    }
  }

  def compileSqlQueryFilters(filterListsIn: util.List[util.List[Filter]],
                             filterListsOut: util.List[util.List[Filter]]) : Unit = {
    // id > 5 and id < 10 ?
    // (id > 5 and id < 10) or (id > 15 and id < 20) ?
    // id == 5 or id == 10 or id == 15
    // now, do not support id in [5, 10, 15]

    val pendingList = new util.ArrayList[util.List[Filter]]()

    filterListsIn.asScala.foreach(tt => {
      var isCompFilterFlag = false
      val cur = tt
      val loop = new Breaks
      loop.breakable {
        cur.asScala.foreach(t => {
          if (isComposeFilter(t)) {
            pendingList.add(cur)
            isCompFilterFlag = true
            loop.break()
          }
        })
      }

      if (!isCompFilterFlag) {
        filterListsOut.add(cur)
      }
    })

    if (pendingList.size() == 0) {
      // nothing to do for compose filter, and return
      return
    }

    filterListsIn.clear()

    pendingList.asScala.foreach(tt => {
      val fs : util.List[Filter] = tt

      var f0 : Filter = null
      var f01 : Filter = null
      var f02 : Filter = null

      val loop = new Breaks;
      var isReplicated = false
      loop.breakable {
        fs.asScala.foreach(t => {
          t match {
            case Or(f1, f2) => {
              isReplicated = true
              f0 = t
              f01 = f1
              f02 = f2
              loop.break()
            }
            case And(f1, f2) => {
              f0 = t
              f01 = f1
              f02 = f2
              loop.break()
            }
            case _ =>
          }
        })
      }

      if (f0 == null || f01 == null || f02 == null) {
        throw new RuntimeException(" compileFilter has unknown exception : f == null || f01 == null || f02 == null ")
      }

      fs.remove(f0)

      if (isReplicated) {
        val newfs = new util.ArrayList[Filter]()
        fs.asScala.foreach(t => {
          newfs.add(t)
        })

        fs.add(f01)
        newfs.add(f02)

        filterListsIn.add(newfs)
      } else {
        fs.add(f01)
        fs.add(f02)
      }

      filterListsIn.add(fs)
    })

    compileSqlQueryFilters(filterListsIn, filterListsOut)
  }

  def transferFilter(f : Filter) : Option[FieldValuePair] = {
    //这里，attr可能是组合属性：例如：arr[0].x.y
    //对于组合属性，需要考虑
    Option(f match {
      case EqualTo(attr, value) => {
        new FieldValuePair(attr, value.toString, FieldValuePair.FieldCompareType.Cmp_Equal)
      }
      case LessThan(attr, value) => {
        new FieldValuePair(attr, value.toString, FieldValuePair.FieldCompareType.Cmp_Less)
      }
      case LessThanOrEqual(attr, value) => {
        new FieldValuePair(attr, value.toString, FieldValuePair.FieldCompareType.Cmp_Less)
      }
      case GreaterThan(attr, value) => {
        new FieldValuePair(attr, value.toString, FieldValuePair.FieldCompareType.Cmp_Greater)
      }
      case GreaterThanOrEqual(attr, value) => {
        new FieldValuePair(attr, value.toString, FieldValuePair.FieldCompareType.Cmp_Greater)
      }
      case _ => null
    })
  }

  def compileSqlQueryFilterGroup(filterList: util.List[Filter]) : util.List[FieldValuePair] = {
    val fieldValuePairs = new util.ArrayList[FieldValuePair]()
    filterList.toArray.asInstanceOf[Array[Filter]]
      .map(tf => transferFilter(tf))
      .filter(tf => (tf.get != null))
      .foreach(tf => fieldValuePairs.add(tf.get))

    fieldValuePairs
  }

  def transferSqlQueryFilters(filterList: util.List[util.List[Filter]]) : util.List[util.List[FieldValuePair]] = {
    val filedValuePairList = new util.ArrayList[util.List[FieldValuePair]]()

    filterList.asScala.foreach(tt => {
      val fieldValuePairs = new util.ArrayList[FieldValuePair]()
      tt.asScala.foreach(tf => {
        val tft = transferFilter(tf)
        if (tft.get != null) {
          fieldValuePairs.add(tft.get)
        }
      })

      if (fieldValuePairs.size() > 0) {
        filedValuePairList.add(fieldValuePairs)
      }
    })

    filedValuePairList
  }

  def findFieldValuePairGroupByAttr(attr:String, filters : util.List[FieldValuePair]) : util.List[FieldValuePair] = {
    val out = new util.ArrayList[FieldValuePair]()
    filters.asScala.foreach(t => {
      if (attr.equals(t.getField)) {
        out.add(t)
      }
    })

    out
  }

  /*
  * cmp : -1 is less; 0 is equal; 1 is greater;
  *
  * */
  def buildHBaseRowkeyFilter(t: RowKeyTransfer,
                             rkfields:Array[String],
                             partfieldvalues:util.List[FieldValuePair],
                             cmp: FieldCompareType) : org.apache.hadoop.hbase.filter.Filter = {
    val rk = t.transferPartial(partfieldvalues)

    //TODO: 更多 rowkey 组合形式待支持

    val outf = cmp match {
      case FieldCompareType.Cmp_Equal => {
        //如果 rowkey 的所有字段，在 sql filters 都有条件，而且都是 Equal 条件，说明是点查询
        //否则，说明是前缀匹配查询
        //TODO: to be tested
        if (rkfields.length == partfieldvalues.size()) {
          new org.apache.hadoop.hbase.filter.RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(rk)))
        } else {
          new org.apache.hadoop.hbase.filter.RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(rk)))
        }
      }
      case FieldCompareType.Cmp_Greater => new org.apache.hadoop.hbase.filter.RowFilter(CompareFilter.CompareOp.GREATER,
        new BinaryComparator(Bytes.toBytes(rk)))
      case FieldCompareType.Cmp_GreaterAndEqual => new org.apache.hadoop.hbase.filter.RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes(rk)))
      case FieldCompareType.Cmp_Less => new org.apache.hadoop.hbase.filter.RowFilter(CompareFilter.CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes(rk)))
      case FieldCompareType.Cmp_LessAndEqual => new org.apache.hadoop.hbase.filter.RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes(rk)))
    }
    outf
  }

  def reverseFieldCompareType(fct : FieldCompareType) : FieldCompareType = {
    val out = fct match {
      case FieldCompareType.Cmp_Equal => FieldCompareType.Cmp_Equal
      case FieldCompareType.Cmp_Greater => FieldCompareType.Cmp_Less
      case FieldCompareType.Cmp_GreaterAndEqual => FieldCompareType.Cmp_LessAndEqual
      case FieldCompareType.Cmp_Less => FieldCompareType.Cmp_Greater
      case _ => FieldCompareType.Cmp_GreaterAndEqual
    }

    out
  }

  def cloneList[T](input : util.List[T]) : util.List[T] = {
    val out = new util.ArrayList[T](input)

    out
  }

  def compileFieldValuePairGroupForRowFilter(t: RowKeyTransfer, filters : util.List[FieldValuePair]) : org.apache.hadoop.hbase.filter.FilterList = {
    val outfilterlist = new org.apache.hadoop.hbase.filter.FilterList(org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL)

    val rowkeyFields: Array[String] = t.rowkeyFieldArray()
    val partFields = new util.ArrayList[String]()
    val partFieldValues = new util.ArrayList[FieldValuePair]()

    val partFieldValueList = new util.ArrayList[util.List[FieldValuePair]]()

    // rowkey(in RowKeyTransfer) 字段必须是顺序出现,
    // RowKeyTransfer 一定包含所有的 rowkey 字段，否则无法 compose rowkey
    var isBreak = true
    val loop = new Breaks;
    loop.breakable {
      rowkeyFields.foreach(rkfield => {
        val fs = findFieldValuePairGroupByAttr(rkfield, filters)
        //there is no filter in filters for rowkey field
        if (fs.size() == 0) {
          loop.break()
        }
        // there is only one filter in filters for rowkey field
        // if filter whthin rowkey fields is equal operation, continue
        // else if filter is < or > operation, break search progress
        else if (fs.size() == 1) {
          val f0 = fs.get(0)
          f0.getCompareType match {
            case FieldCompareType.Cmp_Equal => {
              partFieldValues.add(f0)
              isBreak = false
            }
            case _ => {
              t.fieldSortType(f0.getField) match {
                case RowKeyTransfer.RowkeyFieldSortType.NONE => {
                  logWarning(f0.getField + " is unsorted rowkey field")
                }
                case RowKeyTransfer.RowkeyFieldSortType.ASC => {
                  partFieldValues.add(f0)
                  outfilterlist.addFilter(buildHBaseRowkeyFilter(t,rowkeyFields,partFieldValues,f0.getCompareType))
                }
                case RowKeyTransfer.RowkeyFieldSortType.DESC => {
                  partFieldValues.add(f0)
                  outfilterlist.addFilter(buildHBaseRowkeyFilter(t,rowkeyFields,partFieldValues,
                    reverseFieldCompareType(f0.getCompareType)))
                }
              }
              isBreak = true
              loop.break()
            }
          }
        }
        // if there are many filters for rowkey field in filters
        // operation must be < or > ( because it is AND compression, not OR)
        else {
          fs.asScala.foreach(f0 => {
            f0.getCompareType match {
              case FieldCompareType.Cmp_Equal => {
                logWarning(f0.getField + " is rowkey field, should not have (== and > or <) at the same time")
                loop.break()
              }
              case _ => {
                t.fieldSortType(f0.getField) match {
                  case RowKeyTransfer.RowkeyFieldSortType.NONE => {
                    logWarning(f0.getField + " is unsorted rowkey field")
                    loop.break()
                  }
                  case RowKeyTransfer.RowkeyFieldSortType.ASC => {
                    val newfsvs = cloneList(partFieldValues)
                    newfsvs.add(f0)
                    outfilterlist.addFilter(buildHBaseRowkeyFilter(t, rowkeyFields, newfsvs, f0.getCompareType))
                  }
                  case RowKeyTransfer.RowkeyFieldSortType.DESC => {
                    val newfsvs = cloneList(partFieldValues)
                    newfsvs.add(f0)
                    outfilterlist.addFilter(buildHBaseRowkeyFilter(t, rowkeyFields, newfsvs,
                      reverseFieldCompareType(f0.getCompareType)))
                  }
                }
              }
            }
          })

          isBreak = true
          loop.break()
        }
      })
    }

    if (!isBreak && partFieldValues.size() > 0) {
      outfilterlist.addFilter(buildHBaseRowkeyFilter(t,rowkeyFields,partFieldValues,FieldCompareType.Cmp_Equal))
    }

    outfilterlist
  }

  def transferFieldValuePairToValueCondition(fieldValuePair: FieldValuePair) : ValueCondition = {
    val out = fieldValuePair.getCompareType match {
      case FieldCompareType.Cmp_Equal => new EqualCompator(fieldValuePair.getField, fieldValuePair.getValue.toString)
      case FieldCompareType.Cmp_Greater => new GreaterCompator(fieldValuePair.getField, fieldValuePair.getValue.toString)
      case FieldCompareType.Cmp_GreaterAndEqual => new GreaterEqualCompator(fieldValuePair.getField, fieldValuePair.getValue.toString)
      case FieldCompareType.Cmp_Less => new LessCompator(fieldValuePair.getField, fieldValuePair.getValue.toString)
      case FieldCompareType.Cmp_LessAndEqual => new LessEqualCompator(fieldValuePair.getField, fieldValuePair.getValue.toString)
      case _ => new EqualCompator(fieldValuePair.getField, fieldValuePair.getValue.toString)
    }

    out
  }

  def transferFieldValuePairGroupToValueCondition(filters : util.List[FieldValuePair]) : ValueCondition = {
    val vcs = new Array[ValueCondition](filters.size())
    var id = 0
    filters.asScala.foreach(t => {
      vcs(id) = transferFieldValuePairToValueCondition(t)
      id = id + 1
    })

    new ValueAndCondition(vcs)
  }

  def compileFieldValuePairGroupsForValueCondition(filterLists : util.List[util.List[FieldValuePair]]) : ValueCondition = {
    val vcs = new Array[ValueCondition](filterLists.size())
    var id = 0
    //TODO: 待优化...
    filterLists.asScala.foreach(t => {
      vcs(id) = transferFieldValuePairGroupToValueCondition(t)
      id = id + 1
    })

    //如果没有 filter ， ValueOrCondition 为 NULL
    if (vcs.length > 0) {
      new ValueOrCondition(vcs)
    } else {
      null
    }
  }

  def travelSqlQueryFilters(filters : Array[Filter]) : util.ArrayList[util.List[Filter]] = {
    val outFilterLists = new util.ArrayList[util.List[Filter]]()

    for (filter <- filters) {
      val filterList = new util.ArrayList[Filter]()

      filterList.add(filter)

      outFilterLists.add(filterList)
    }

    outFilterLists
  }

  def buildRowkeyAndValueFilter(rowKeyTransfer : RowKeyTransfer, filters: Array[Filter]) : (org.apache.hadoop.hbase.filter.Filter, ValueCondition) = {
    val filterLists = travelSqlQueryFilters(filters)
    val compiledFilterLists = new util.ArrayList[util.List[Filter]]()
    compileSqlQueryFilters(filterLists, compiledFilterLists)

    val fieldValueLists = transferSqlQueryFilters(compiledFilterLists)
    val hbaseRowFilterList = new filter.FilterList(org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ONE)

    fieldValueLists.asScala.foreach(tt => {
      val filterlist = compileFieldValuePairGroupForRowFilter(rowKeyTransfer, tt)
      //TODO: 需要去重
      hbaseRowFilterList.addFilter(filterlist)
    })

    val valueCondition = compileFieldValuePairGroupsForValueCondition(fieldValueLists)

    if (hbaseRowFilterList.getFilters.size() > 0) {
      (hbaseRowFilterList, valueCondition)
    } else {
      (null, valueCondition)
    }
  }

  def scanTable(sc : SparkContext,
                requiredColumns: Array[String],
                filters: Array[Filter],
                parts: Array[Partition],
                hbaseOptions: HBaseOptions) : RDD[InternalRow] = {
    val (hbaseFilterList, valueCondition) = buildRowkeyAndValueFilter(hbaseOptions.rowkeyTransfer, filters)

    val avroSchema = new Schema.Parser().parse(hbaseOptions.tableAvsc)
    val schema = AvroSchema.transferAvroSchema(avroSchema)

    val avroFilter = new SingleAvroFilter(hbaseOptions.family.getBytes(Charset.forName("utf-8")),
      hbaseFilterList, valueCondition, hbaseOptions.tableAvsc)

    new HBaseRDD_0(sc, hbaseOptions, schema.asInstanceOf[StructType], avroFilter)
  }

//  def scanTable1(sc : SparkContext,
//                requiredColumns: Array[String],
//                filters: Array[Filter],
//                parts: Array[Partition],
//                hbaseOptions: HBaseOptions) : RDD[InternalRow] = {
//    val filterLists = travelSqlQueryFilters(filters)
//
//    val compiledFilterLists = new util.ArrayList[util.List[Filter]]()
//
//    compileSqlQueryFilters(filterLists, compiledFilterLists)
//
//    val fieldValueLists = transferSqlQueryFilters(compiledFilterLists)
//
//    val rowKeyTransfer = hbaseOptions.rowkeyTransfer
//
//    val hbaseFilterList = new filter.FilterList(org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ONE)
//    val hbaseRowFilterList = new util.ArrayList[org.apache.hadoop.hbase.filter.FilterList]()
//
//    fieldValueLists.asScala.foreach(tt => {
//      val filterlist = compileFieldValuePairGroupForRowFilter(rowKeyTransfer, tt)
//      //TODO: 需要去重
//      hbaseFilterList.addFilter(filterlist)
//    })
//
//    val valueCondition = compileFieldValuePairGroupsForValueCondition(fieldValueLists)
//
//    val thriftFilter = new SingleThriftFilter(hbaseOptions.family.getBytes(Charset.forName("utf-8")),
//      hbaseFilterList, valueCondition, null/*hbaseOptions.valueClass*/)
//
//    //val schema = HBaseUtils.convertThriftSchema(hbaseOptions.valueClassName)
//    val avroSchema = new Schema.Parser().parse(hbaseOptions.tableAvsc)
//    val schema = AvroSchema.transferAvroSchema(avroSchema)
//
//    new HBaseRDD(sc, hbaseOptions, schema.asInstanceOf[StructType], thriftFilter)
//  }
}

class HBaseRDD_0(sc: SparkContext,
               options: HBaseOptions,
               schema : StructType,
               avroFilter : SingleAvroFilter)
  extends RDD[InternalRow](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    var closed = false
    var conn : Connection = null
    var tableInterface : Table = null
    var resultScanner : ResultScanner = null

    def close() {
      logInfo(" ***********************  HBaseRDD : compute : close  ************************ ")

      if (closed) return
      try {
        resultScanner.close()
      } catch {
        case e : Exception => logWarning("hbase result scanner close", e)
      }

      try {
        if (tableInterface != null) {
          tableInterface.close()
        }
      } catch {
        case e : Exception => logWarning("hbase table close ", e)
      }

      try {
        if (conn != null) {
          conn.close()
        }
      } catch {
        case e : Exception => logWarning("hbase connection close ", e)
      }

      logInfo("closed connection")
      closed = true
    }

    context.addTaskCompletionListener{ context => close() }

    val scan = new Scan

    //TODO : scan.setMaxVersions.setBatch(2).setCaching(hbaseOptions.)
    //TODO : 列裁剪
    scan.setFilter(avroFilter)
    scan.setCaching(1000)

    try {
      conn = ConnectionFactory.createConnection(HBaseUtils.getConfiguration(options))
      tableInterface = conn.getTable(TableName.valueOf(options.table))
      resultScanner = tableInterface.getScanner(scan)

      val rowsIterator = HBaseUtils.resultSetToSparkInternalRows(options, resultScanner, schema)
      CompletionIterator[InternalRow, Iterator[InternalRow]](
        new InterruptibleIterator(context, rowsIterator), close())
    } catch {
      case e : IOException => logError(e.getMessage, e); throw e
    }
  }

  override protected def getPartitions: Array[Partition] = {
    HBaseUtils.getPartitions(options)
  }
}
