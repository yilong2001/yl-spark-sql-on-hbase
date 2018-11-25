package com.example.spark.sql.execution.thrift

import java.util

import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.thrift.TFieldIdEnum
import org.apache.thrift.meta_data._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yilong on 2018/6/25.
  */
object ThriftSchemaPlain {
  def converThriftSchemaToPlain(thriftCls : Class[_]): StructType = {
    val mdm =  ThriftSchema.getThriftMetaDataMap(thriftCls)

    val outFieldsArray = convertThriftSchemaToPlainInternal(null, mdm)

    new StructType(outFieldsArray)
  }

  private def convertThriftSchemaToPlainInternal(prefix: String, mdm : util.Map[Object, Object]) : Array[StructField] = {
    var outFieldsArrayBuf = new ArrayBuffer[StructField]()
    val catStr = "."

    mdm.entrySet().toArray.foreach(entry =>{
      val et = entry.asInstanceOf[util.Map.Entry[_ <: TFieldIdEnum, FieldMetaData]]

      val fieldMetaData = et.getValue

      val fieldValueMetaData = et.getValue.valueMetaData

      var newprefix = prefix
      if (prefix != null && !prefix.equals("")) {
        newprefix = prefix+catStr+fieldMetaData.fieldName
      } else {
        newprefix = fieldMetaData.fieldName
      }

      if (fieldValueMetaData.isInstanceOf[StructMetaData]) {
        val smd = fieldValueMetaData.asInstanceOf[StructMetaData]

        val submdm = ThriftSchema.getThriftMetaDataMap(smd.structClass)

        outFieldsArrayBuf ++= convertThriftSchemaToPlainInternal(newprefix, submdm)
      } else if (fieldValueMetaData.isInstanceOf[ListMetaData]) {
        val smd = fieldValueMetaData.asInstanceOf[ListMetaData]

        val cls = smd.elemMetaData.asInstanceOf[StructMetaData]

        newprefix = newprefix + "()"

        val submdm = ThriftSchema.getThriftMetaDataMap(cls.structClass)

        outFieldsArrayBuf ++= convertThriftSchemaToPlainInternal(newprefix, submdm)
      } else if (fieldValueMetaData.isInstanceOf[SetMetaData]) {
        val smd = fieldValueMetaData.asInstanceOf[SetMetaData]
        val cls = smd.elemMetaData.asInstanceOf[StructMetaData]

        newprefix = newprefix + "()"

        val submdm = ThriftSchema.getThriftMetaDataMap(cls.structClass)

        outFieldsArrayBuf ++= convertThriftSchemaToPlainInternal(newprefix, submdm)
      } else if (fieldValueMetaData.isInstanceOf[MapMetaData]) {
        val smd = fieldValueMetaData.asInstanceOf[MapMetaData]

        val subfieldvaluemeta = smd.valueMetaData

        newprefix = newprefix + "[]"

        val isRequired = fieldMetaData.requirementType.equals(org.apache.thrift.TFieldRequirementType.REQUIRED)

        outFieldsArrayBuf ++= convertThriftSchemaToPlainInternal(newprefix, isRequired, subfieldvaluemeta)
      } else {
        val metadata = new MetadataBuilder()
          .putString("name", newprefix)
          .putLong("scale", 0)
        val columnType = ThriftSchema.getCatalystType(fieldMetaData.valueMetaData.`type`)
        val nullable = (!fieldMetaData.requirementType.equals(org.apache.thrift.TFieldRequirementType.REQUIRED))
        val fields = StructField(newprefix, columnType, nullable, metadata.build())

        outFieldsArrayBuf ++= Array[StructField](fields)
      }
    })

    outFieldsArrayBuf.toArray
  }

  private def convertThriftSchemaToPlainInternal(prefix: String, isRequire : Boolean, fieldValueMetaData: FieldValueMetaData) : Array[StructField] = {
    var outFieldsArrayBuf = new ArrayBuffer[StructField]()

    if (fieldValueMetaData.isInstanceOf[StructMetaData]) {
      val smd = fieldValueMetaData.asInstanceOf[StructMetaData]
      val submdm = ThriftSchema.getThriftMetaDataMap(smd.structClass)

      outFieldsArrayBuf ++= convertThriftSchemaToPlainInternal(prefix, submdm)
    } else if (fieldValueMetaData.isInstanceOf[ListMetaData]) {
      val smd = fieldValueMetaData.asInstanceOf[ListMetaData]
      val cls = smd.elemMetaData.asInstanceOf[StructMetaData]

      var newprefix = prefix+"()"

      val submdm = ThriftSchema.getThriftMetaDataMap(cls.structClass)

      outFieldsArrayBuf ++= convertThriftSchemaToPlainInternal(newprefix, submdm)
    } else if (fieldValueMetaData.isInstanceOf[SetMetaData]) {
      val smd = fieldValueMetaData.asInstanceOf[SetMetaData]
      val cls = smd.elemMetaData.asInstanceOf[StructMetaData]

      var newprefix = prefix+"()"

      val submdm = ThriftSchema.getThriftMetaDataMap(cls.structClass)

      outFieldsArrayBuf ++= convertThriftSchemaToPlainInternal(newprefix, submdm)
    } else if (fieldValueMetaData.isInstanceOf[MapMetaData]) {
      val smd = fieldValueMetaData.asInstanceOf[MapMetaData]

      val subfieldvaluemeta = smd.valueMetaData

      var newprefix = prefix + "[]"

      convertThriftSchemaToPlainInternal(newprefix, isRequire, subfieldvaluemeta)
    } else {
      val metadata = new MetadataBuilder()
        .putString("name", prefix)
        .putLong("scale", 0)
      val columnType = ThriftSchema.getCatalystType(fieldValueMetaData.`type`)
      val nullable = !(isRequire)
      val fields = StructField(prefix, columnType, nullable, metadata.build())

      outFieldsArrayBuf += (fields)
    }

    outFieldsArrayBuf.toArray
  }
}
