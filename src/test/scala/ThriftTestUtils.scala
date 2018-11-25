import java.util
import java.util.function.Consumer

import com.example.spark.demo.generated.thrift.{ExecutorInfo, NodeInfo, TopoNodeAssignment, WorkerResources}
import com.example.spark.sql.execution.hbase.HBaseOptions.{HBASE_TABLE_FAMILY_NAME, HBASE_TABLE_NAME, HBASE_TABLE_ROWKEY_TRANSFER, HBASE_TABLE_VALUE_CLASS_NAME}
import com.example.spark.sql.execution.hbase.{HBaseRDD, HBaseRelationProvider}
import com.example.spark.sql.execution.thrift.{ThriftConverter, ThriftSchema}
import com.example.spark.sql.util.Serde
import org.apache.hadoop.hbase.filter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources._
import org.apache.thrift.TFieldIdEnum
import org.apache.thrift.meta_data.{FieldMetaData, ListMetaData, StructMetaData}

/**
  * Created by yilong on 2018/6/25.
  */
object ThriftTestUtils {
  def testDataTypeJsonBuilder() : Unit = {
    val top = new TopoNodeAssignment
    val cls = Class.forName(top.getClass.getName)
    println(cls.getName)

    val obj = cls.newInstance()

    val field = cls.getDeclaredField("metaDataMap")
    field.setAccessible(true)

    val mdm =  field.get(obj).asInstanceOf[util.Map[Object, Object]]

    mdm.entrySet().toArray.foreach(entry =>{
      val et = entry.asInstanceOf[util.Map.Entry[_ <: TFieldIdEnum, FieldMetaData]]
      println(et.getKey.getFieldName)
      if (et.getValue.valueMetaData.isInstanceOf[StructMetaData]) {
        println("================ StructMetaData ===================")
        val smd = et.getValue.valueMetaData.asInstanceOf[StructMetaData]
        println(smd.structClass.getName)
        println(smd.`type`.toInt)
        println(et.getValue.fieldName)
      } else if (et.getValue.valueMetaData.isInstanceOf[ListMetaData]) {
        println("================ ListMetaData ===================")
        val smd = et.getValue.valueMetaData.asInstanceOf[ListMetaData]
        val cls = smd.elemMetaData.asInstanceOf[StructMetaData]
        println(cls.`type`.toInt)
        println(cls.structClass.getName)
      } else {
        println(et.getValue.fieldName)
        println(et.getValue.valueMetaData.getTypedefName)
        println(et.getValue.valueMetaData.`type`.toInt)
      }
    })
  }

  def testGetSqlSchemaFromThrift() : Unit = {
    val top = new TopoNodeAssignment
    val cls = Class.forName(top.getClass.getName)

    val structType = ThriftSchema.convertThriftSchema(cls)

    println(structType.catalogString)
  }

  def testNodeInfoThriftConvert() : Unit = {
    val ni = new NodeInfo()
    ni.setNode("testnode")
    val set : util.Set[java.lang.Long] = new util.HashSet()
    set.add(100L)
    set.add(200L)
    ni.setPort(set)

    val cls = Class.forName(ni.getClass.getName)
    val structType = ThriftSchema.convertThriftSchema(cls)

    val row = ThriftConverter.convert(ni, structType)

    println(row.toString)
  }

  def testTopAssignThriftConvert() : Unit = {
    val top = new TopoNodeAssignment()
    top.setTopology_id("test_top")

    val exec1 = new ExecutorInfo()
    exec1.setTask_start(101)
    exec1.setTask_end(105)
    exec1.setStart_time_secs(100)

    val exec2 = new ExecutorInfo()
    exec2.setTask_start(201)
    exec2.setTask_end(205)

    val execs = new util.ArrayList[ExecutorInfo]()
    execs.add(exec1)
    execs.add(exec2)

    top.setExecutors(execs)

    val res = new WorkerResources()
    res.setCpu(100.1)
    res.setMem_off_heap(200.2)
    res.setMem_on_heap(300.3)
    top.setResources(res)

    val cls = Class.forName(top.getClass.getName)
    val structType = ThriftSchema.convertThriftSchema(cls)

    val row = ThriftConverter.convert(top, structType)

    println(row)
  }


  def main(args: Array[String]): Unit = {

  }
}
