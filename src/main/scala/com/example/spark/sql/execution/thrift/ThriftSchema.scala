package com.example.spark.sql.execution.thrift

import java.sql.SQLException
import java.util

import com.example.spark.sql.util.ClassUtilInScala
import org.apache.spark.sql.types._
import org.apache.thrift.TFieldIdEnum
import org.apache.thrift.meta_data._

import scala.collection.mutable.ArrayBuffer

import scala.collection.JavaConverters._

/**
  * Created by yilong on 2018/6/25.
  */
object ThriftSchema {
  def getThriftMetaDataMap(cls : Class[_]) : util.Map[Object, Object] = {
    println("********************** : "+cls.getName)
    val field = cls.getDeclaredField("metaDataMap")
    field.setAccessible(true)

    val obj = cls.newInstance()

    field.get(obj).asInstanceOf[util.Map[Object, Object]]
  }

  def getThriftMetaDataMap(obj : Object) : util.Map[Object, Object] = {
    val field = obj.getClass().getDeclaredField("metaDataMap")
    field.setAccessible(true)

    field.get(obj).asInstanceOf[util.Map[Object, Object]]
  }

  def getCatalystType(fieldType: Byte): DataType = {
    val answer = fieldType match {
      // scalastyle:off
      case org.apache.thrift.protocol.TType.BOOL => BooleanType
      case org.apache.thrift.protocol.TType.BYTE => BinaryType
      case org.apache.thrift.protocol.TType.DOUBLE => DoubleType
      //TODO : case org.apache.thrift.protocol.TType.ENUM =>
      case org.apache.thrift.protocol.TType.I16 => ShortType
      case org.apache.thrift.protocol.TType.I32 => IntegerType
      case org.apache.thrift.protocol.TType.I64 => LongType
      case org.apache.thrift.protocol.TType.STRING => StringType
      //TODO:
      case org.apache.thrift.protocol.TType.LIST => null
      case org.apache.thrift.protocol.TType.MAP => null
      case org.apache.thrift.protocol.TType.SET => null
      case org.apache.thrift.protocol.TType.STRUCT => null
      case org.apache.thrift.protocol.TType.STOP => null
      case _                            =>
        throw new SQLException("Unrecognized thrift field type : " + fieldType)
      // scalastyle:on
    }

    if (answer == null) {
      throw new SQLException("Unsupported thrift field type : " + fieldType)
    }
    answer
  }

  def convertThriftSchema(thriftClsName: String): StructType = {
    val cls = ClassUtilInScala.classForName(thriftClsName)
    convertThriftSchema(cls)
  }

  def convertThriftSchema(thriftCls: Class[_]): StructType = {
    var outFieldsArrayBuf = new ArrayBuffer[StructField]()

    val mdm = ThriftSchema.getThriftMetaDataMap(thriftCls)

    val fields = mdm.entrySet().asScala.toList.map(entry => {
      val et = entry.asInstanceOf[util.Map.Entry[_ <: TFieldIdEnum, FieldMetaData]]

      val fieldMetaData = et.getValue

      val fieldValueMetaData = et.getValue.valueMetaData

      val nullable = (!fieldMetaData.requirementType.equals(org.apache.thrift.TFieldRequirementType.REQUIRED))

      val dataType = convertFieldMetaData(fieldValueMetaData, nullable)

      StructField(fieldMetaData.fieldName, dataType, nullable)
    })

    StructType(fields)
  }

  def convertFieldMetaData(fieldValueMetaData: FieldValueMetaData, nullable: Boolean): DataType = {
    fieldValueMetaData match {
      case t: StructMetaData => {
        convertThriftSchema(t.structClass)
      }
      case t: ListMetaData => {
        val emd = t.elemMetaData

        ArrayType(convertFieldMetaData(emd, nullable), containsNull = nullable)
      }
      case t: SetMetaData => {
        val emd = t.elemMetaData

        ArrayType(convertFieldMetaData(emd, nullable), containsNull = nullable)
      }
      case t: MapMetaData => {
        val keymetadata = t.keyMetaData

        val valuemetadata = t.valueMetaData

        MapType(
          convertFieldMetaData(keymetadata, nullable),
          convertFieldMetaData(valuemetadata, nullable),
          valueContainsNull = nullable)
      }
      case _ => {
        ThriftSchema.getCatalystType(fieldValueMetaData.`type`)
      }
    }
  }

}
