package com.example.spark.sql.execution.hbase

import java.nio.ByteBuffer

import com.example.spark.hbase.coprocessor.SingleSqlCountStatis
import com.example.spark.sql.execution.avro.AvroSchema
import com.example.spark.sql.util.HBaseUtils
import org.apache.avro.Schema
import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

/**
  * Created by yilong on 2018/6/11.
  */
case class HBasePartitioningInfo(splits: List[Array[Byte]])

object HBaseRelation extends Logging {
  /**
    * @param partitioningInfo partition information to generate the where clause for each partition
    * @return an array of partitions with where clause for each partition
    */
  def columnPartition(partitioningInfo: HBasePartitioningInfo): Array[Partition] = {
    logInfo(" ***********************  columnPartition  ************************ ")
    if (partitioningInfo == null || partitioningInfo.splits.size == 0) {
      return Array[Partition](HBasePartition(null, null, 0))
    }
    var id = -1
    val out = partitioningInfo.splits.map(btarr => {
      id=id+1
      val start = (id==0) match {case true => null; case _ => new String(partitioningInfo.splits.apply(id-1))}
      HBasePartition(start, new String(btarr), id)
    })

    out.toArray
  }
}

case class HBaseRelation(parts: Array[Partition],
                         hbaseOptions: HBaseOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  // Check if HBASERDD.compileFilter can accept input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    //TODO: unhandled filters
    filters.filter(f=>false)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    HBaseRDD.scanTable(
      sparkSession.sparkContext,
      requiredColumns,
      filters,
      parts,
      hbaseOptions).asInstanceOf[RDD[Row]]
  }

  override def toString: String = {
    val partitioningInfo = if (parts.nonEmpty) s" [numPartitions=${parts.length}]" else ""
    // credentials should not be included in the plan output, table information is sufficient.
    s"HBaseRelation(${hbaseOptions.table})" + partitioningInfo
  }

  override def schema: StructType = AvroSchema.transferAvroSchema(new Schema.Parser().parse(hbaseOptions.tableAvsc)).asInstanceOf[StructType]
  //HBaseUtils.convertThriftSchema(hbaseOptions.valueClassName)

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    //TODO: hbase insert
  }

  var bsize = 0L;
  override def sizeInBytes() : Long = {
    bsize
  }
//  def updateStatistic(statis : SingleSqlCountStatis) : Unit = {
//    this.sizeInBytes = statis.byteSize
//  }
}