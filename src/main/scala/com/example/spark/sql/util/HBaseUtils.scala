package com.example.spark.sql.util

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.charset.Charset
import java.sql.SQLException
import java.util

import com.example.spark.sql.execution.avro.AvroConverter
import com.example.spark.sql.execution.hbase.{HBaseOptions, HBasePartition}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by yilong on 2018/6/8.
  */
object HBaseUtils extends Logging {
  // hbase table description : hbase 结构化表结构 需要使用独立的元数据管理方式(类似于 json 结构)
  // 如果要能够管理 表结构，必须实现 create table 接口

  def getPartitions(options: HBaseOptions): Array[Partition] = {
    var conn: Connection = null
    val partitionArr = ArrayBuffer[HBasePartition]()
    try {
      conn = ConnectionFactory.createConnection(getConfiguration(options))

      val rl = conn.getRegionLocator(TableName.valueOf(options.table))
      var id = 0
      if (rl == null || rl.getEndKeys == null || rl.getEndKeys.length == 0) {
        logError(options.table + ", get region locator, but NULL. ")
        throw new IOException(options.table + ", get region locator, but NULL (or endkeys is empty). ")
      }

      for (key : Array[Byte] <- rl.getEndKeys) {
        logInfo("rowkey : *****************************************")
        if (key == null || key.length == 0) {
          //TODO:
          partitionArr += HBasePartition(null, null, id)
        }
        else {
          val start = (id==0)match {case true=>null; case _=> new String(rl.getEndKeys.apply(id))}
          partitionArr += HBasePartition(start, new String(key), id)
          id = id + 1
        }
      }

      partitionArr.toArray
    } catch {
      case e: IOException => {
        logError(e.getMessage, e)
        partitionArr.clear()
        partitionArr += HBasePartition(null, null, 0)

        partitionArr.toArray
      }
    } finally {
      if (conn != null) {
        try
          conn.close()
        catch {
          case e: IOException => logError(e.getMessage, e)
        } finally conn = null
      }
    }
  }

  def resultSetToSparkInternalRows(options: HBaseOptions,
                                   resultScanner: ResultScanner,
                                   schema: StructType): Iterator[InternalRow] = {
    new NextIterator[InternalRow] {
      private[this] val rs = resultScanner

      val avroSchema = new Schema.Parser().parse(options.tableAvsc)
      val avroDatumReader = new GenericDatumReader[GenericRecord](avroSchema)

      override protected def close(): Unit = {
        try {
          rs.close()
        } catch {
          case e: Exception => logWarning("Exception closing resultset", e)
        }
      }

      override protected def getNext(): InternalRow = {
        val result = resultScanner.next()
        if (result != null) {
          val cells = result.getColumnCells(options.family.getBytes(Charset.forName("utf-8")),
            options.family.getBytes(Charset.forName("utf-8")))
          if (cells == null || cells.size() == 0) {
            finished = true
            null.asInstanceOf[InternalRow]
          } else {
            //TODO: how to support multi cell in cells
            //val data = cells.get(0).getValueArray
            //val obj = ThriftSerde.deSerialize(new NodeInfo(null, null).getClass/*options.valueClass*/, data)

            val keyValue = result.listCells().get(0)
            if (keyValue == null) {
              logError("keyvalue in cells(0) should not be null")
              throw new IOException("keyvalue in cells(0) should not be null")
            }

            if (schema == null) {
              logError("avro schema should not be null")
              throw new IOException("avro schema should not be null")
            }
            //val obj = ThriftSerde.deSerialize(options.valueClass, keyValue.getValue)
            //val row = HBaseUtils.convertThriftObject(obj, schema)
            val avroDecoder = DecoderFactory.get.binaryDecoder(keyValue.getValue, null)
            val outResult = avroDatumReader.read(null, avroDecoder)
            val row = AvroConverter.convert(outResult, schema)
            row
          }
        } else {
          finished = true
          null.asInstanceOf[InternalRow]
        }
      }
    }
  }

  def getSplits(options: HBaseOptions) : List[Array[Byte]] = {
    logInfo(" ***********************  getSplits  ************************ ")

    var conn : Connection = null
    var tableIf : Table = null
    var list = List[Array[Byte]]()
    try {
      conn = ConnectionFactory.createConnection(getConfiguration(options))

      val rl = conn.getRegionLocator(TableName.valueOf(options.table))

      for (key : Array[Byte] <- rl.getEndKeys) {
        if (key == null || key.length == 0) logWarning(options.table+" : NULL or len is 0")
        else {
          list = List.concat(list, List(key))
        }
      }

      list
    } catch {
      case e: IOException => logError(e.getMessage, e); List()
    } finally {
      if (conn != null) try
        conn.close()
      catch {
        case e: IOException => logError(e.getMessage, e)
      }
    }
  }

  def getConfiguration(options: HBaseOptions) : Configuration = {
    val conf = HBaseConfiguration.create
    conf
  }
}
