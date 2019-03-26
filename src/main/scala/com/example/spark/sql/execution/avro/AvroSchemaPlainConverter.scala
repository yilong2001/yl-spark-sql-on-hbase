package com.example.spark.sql.execution.avro

import java.util

import org.apache.avro.Schema
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


/**
  * Created by yilong on 2018/6/25.
  * to plain avro : only primitive , record, union, fiexed
  * not support array, map
  */
object AvroSchemaPlainConverter {
  val DEFAULT_CAT_CHAR = "."

  private def updatedPrefix(oldPre:String,catchar:String,curPre:String) : String = {
    if (oldPre == null || oldPre.equals("")) curPre else oldPre+catchar+curPre
  }

  private def getAvroFields(sch : String) : util.List[Schema.Field] = {
    val parser = new Schema.Parser()
    val schema = parser.parse(sch)
    schema.getFields
  }

  private def getCatalystType(schemaType: Schema.Type): DataType = {
    val answer = schemaType match {
      case Schema.Type.INT => IntegerType
      case Schema.Type.DOUBLE => DoubleType
      case Schema.Type.FLOAT => FloatType
      case Schema.Type.LONG => LongType
      case Schema.Type.STRING => StringType
      case Schema.Type.BOOLEAN => BooleanType
      case Schema.Type.ENUM => IntegerType
      case Schema.Type.FIXED => BinaryType
      case Schema.Type.BYTES => BinaryType
      case Schema.Type.NULL => NullType
      //TODO:
      //case Schema.Type.MAP => null
      //case Schema.Type.ARRAY => null
      //case Schema.Type.UNION => null
      //case Schema.Type.RECORD => null
      case _ => throw new IllegalArgumentException("not support union schema with 3 or more items : ${schemaType.getName}")
    }

    answer
  }

  private def getPrincipalTypeFromUnion(schema: Schema) : Schema = {
    (schema.getType) match {
      case Schema.Type.UNION =>
        val subschemas = schema.getTypes
        if (subschemas.size > 2) throw new IllegalArgumentException("not support union schema with 3 or more items : " + schema.getName)
        if (!subschemas.get(0).getType.getName.equals("null") && !subschemas.get(1).getType.getName.equals("null")) throw new IllegalArgumentException("not support union schema(not include null type) : " + schema.getName)
        val basicsch = if (subschemas.get(0).getType.getName.equals("null")) subschemas.get(1) else subschemas.get(0)

        basicsch
      case _ => throw new IllegalArgumentException("not supported union schema : " + schema.toString)
    }
  }

  private def isPrimitiveType(schema: Schema): Boolean = {
    isPrimitiveType(schema.getType)
  }

  private def isPrimitiveType(schemaType: Schema.Type): Boolean = {
    val answer = schemaType match {
      case Schema.Type.INT => true
      case Schema.Type.DOUBLE => true
      case Schema.Type.FLOAT => true
      case Schema.Type.LONG => true
      case Schema.Type.STRING => true
      case Schema.Type.BOOLEAN => true
      case Schema.Type.ENUM => true
      case Schema.Type.FIXED => true
      //TODO:
      case Schema.Type.BYTES => true
      case Schema.Type.NULL => true
      //TODO:
      case Schema.Type.MAP => false
      case Schema.Type.ARRAY => false
      case Schema.Type.UNION => false
      case Schema.Type.RECORD => false
      case _ => throw new IllegalArgumentException("not supported schema : " + schemaType.toString)
    }

    answer
  }

  private def transferAvroUnionSchema(schema: Schema, pre:String, catchar:String, structFields : java.util.List[StructField]) :  Unit = {
    schema.getType match {
      case Schema.Type.UNION =>
        val subch = getPrincipalTypeFromUnion(schema)
        if (isPrimitiveType(subch.getType)) {
          structFields.add(StructField(pre, getCatalystType(subch.getType), true))
        } else {
          val newpre = pre //updatedPrefix(pre, catchar, subch.getName)
          subch.getType match {
            case Schema.Type.RECORD => transferAvroSchema(subch,newpre,catchar,structFields)
            case Schema.Type.ARRAY =>  throw new IllegalArgumentException("not support " + subch.toString + ", in array schema")
            case Schema.Type.MAP =>  throw new IllegalArgumentException("not support " + subch.toString + ", in map schema")
            case _ => throw new IllegalArgumentException("not support " + subch.toString + ", in unknown schema")
          }
        }
      case _ => throw new IllegalArgumentException("not union schema : " + schema.toString)
    }
  }

  private def transferAvroField(field : Schema.Field, pre:String, catchar:String, structFields : java.util.List[StructField]) : Unit = {
    if (isPrimitiveType(field.schema())) {
      structFields.add(StructField(updatedPrefix(pre,catchar,field.name), getCatalystType(field.schema().getType), true))
    } else {
      val newpre = updatedPrefix(pre, catchar, field.name())

      field.schema().getType match {
        case Schema.Type.RECORD =>
          val fields = field.schema().getFields.asScala.foreach(field => {
            transferAvroField(field, newpre, catchar, structFields)
          })
        case Schema.Type.ARRAY => throw new IllegalArgumentException("not supported field : "+field.schema().toString+" is array type!")
        case Schema.Type.MAP => throw new IllegalArgumentException("not supported field : "+field.schema().toString+" is map type!")
        case Schema.Type.UNION => transferAvroUnionSchema(field.schema(), newpre, catchar, structFields)
        case _ => throw new IllegalArgumentException("not support field : " + field.schema().toString)
      }
    }
  }

  def transferAvroSchema(schema:Schema, pre:String, catchar:String, structFields:java.util.List[StructField]) : Unit = {
    if (isPrimitiveType(schema)) {
      structFields.add(StructField(updatedPrefix(pre,catchar,schema.getName), getCatalystType(schema.getType), true))
    } else {
      val newpre = pre //updatedPrefix(pre, catchar, schema.getName)

      schema.getType match {
        case Schema.Type.RECORD =>
          val fields = schema.getFields.asScala.foreach(field => {
            transferAvroField(field, newpre, catchar, structFields)
          })
        case Schema.Type.ARRAY => throw new IllegalArgumentException("not supported field : "+schema.toString+" is array type!")
        case Schema.Type.MAP => throw new IllegalArgumentException("not supported field : "+schema.toString+" is map type!")
        case Schema.Type.UNION => transferAvroUnionSchema(schema, newpre, catchar, structFields)
        case _ => throw new IllegalArgumentException("not support " + schema.toString + ", in map schema")
      }
    }
  }

  def convert(schema : Schema) : StructType = {
    val structFields = new util.ArrayList[StructField]()

    transferAvroSchema(schema, "", DEFAULT_CAT_CHAR, structFields)

    val stArr = new Array[StructField](structFields.size())
    structFields.toArray(stArr)

    StructType(stArr)
  }

  def convert(schemaStr : String)  : StructType = {
    val schema = new Schema.Parser().parse(schemaStr)
    convert(schema)
  }
}
