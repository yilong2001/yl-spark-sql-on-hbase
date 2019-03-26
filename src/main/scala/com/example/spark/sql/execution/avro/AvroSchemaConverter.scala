package com.example.spark.sql.execution.avro

import java.util

import org.apache.avro.Schema
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


/**
  * Created by yilong on 2018/6/25.
  */
object AvroSchemaConverter {
  def getAvroFields(sch : String) : util.List[Schema.Field] = {
    val parser = new Schema.Parser()
    val schema = parser.parse(sch)
    schema.getFields
  }

  def getCatalystType(schemaType: Schema.Type): DataType = {
    val answer = schemaType match {
      case Schema.Type.INT => IntegerType
      case Schema.Type.DOUBLE => DoubleType
      case Schema.Type.FLOAT => FloatType
      case Schema.Type.LONG => LongType
      case Schema.Type.STRING => StringType
      case Schema.Type.BOOLEAN => BooleanType
      case Schema.Type.ENUM => IntegerType
      //TODO:
      case Schema.Type.BYTES => null
      case Schema.Type.MAP => null
      case Schema.Type.NULL => null
      case Schema.Type.ARRAY => null
      case Schema.Type.FIXED => null
      case Schema.Type.UNION => null
      case Schema.Type.RECORD => null
      case _ => throw new IllegalArgumentException("not support union schema with 3 or more items : ${schemaType.getName}")
    }

    answer
  }

  def getElementTypeFromArray(schema: Schema) : Schema = {
    (schema.getType) match {
      case Schema.Type.ARRAY =>
        val subschemas = schema.getElementType

        subschemas
      case _ => throw new IllegalArgumentException("not array schema : " + schema.getName)
    }
  }

  def getPrincipalTypeFromUnion(schema: Schema) : Schema = {
    (schema.getType) match {
      case Schema.Type.UNION =>
        val subschemas = schema.getTypes
        if (subschemas.size > 2) throw new IllegalArgumentException("not support union schema with 3 or more items : " + schema.getName)
        if (!subschemas.get(0).getType.getName.equals("null") && !subschemas.get(1).getType.getName.equals("null")) throw new IllegalArgumentException("not support union schema(not include null type) : " + schema.getName)
        val basicsch = if (subschemas.get(0).getType.getName.equals("null")) subschemas.get(1) else subschemas.get(0)

        basicsch
      case _ => throw new IllegalArgumentException("not union schema : " + schema.getName)
    }
  }

  def isPrimitiveType(schema: Schema): Boolean = {
    isPrimitiveType(schema.getType)
  }

  def isPrimitiveType(schemaType: Schema.Type): Boolean = {
    val answer = schemaType match {
      case Schema.Type.INT => true
      case Schema.Type.DOUBLE => true
      case Schema.Type.FLOAT => true
      case Schema.Type.LONG => true
      case Schema.Type.STRING => true
      case Schema.Type.BOOLEAN => true
      case Schema.Type.ENUM => true
      //TODO:
      case Schema.Type.BYTES => true
      case Schema.Type.NULL => true
      //
      case Schema.Type.MAP => false
      case Schema.Type.ARRAY => false
      //TODO:
      case Schema.Type.FIXED => false
      //TODO:
      case Schema.Type.UNION => false
      case Schema.Type.RECORD => false
      case _ => throw new IllegalArgumentException("not support union schema with 3 or more items : ${schemaType.getName}")
    }

    answer
  }

  def transferAvroArraySchema(schema: Schema) :  DataType = {
    schema.getType match {
      case Schema.Type.ARRAY =>
        val subch = getElementTypeFromArray(schema)
        if (isPrimitiveType(subch.getType)) {
          ArrayType(getCatalystType(subch.getType))
        } else {
          subch.getType match {
            case Schema.Type.RECORD => ArrayType(transferAvroSchema(subch))
            case Schema.Type.ARRAY => transferAvroArraySchema(subch)
            case Schema.Type.MAP => transferAvroMapSchema(subch)
            case Schema.Type.UNION => transferAvroUnionSchema(subch)
            case _ => throw new IllegalArgumentException("not support " + subch.toString + ", in array schema")
          }
        }
      case _ => throw new IllegalArgumentException("not array schema : " + schema.toString)
    }
  }

  def transferAvroUnionSchema(schema: Schema) :  DataType = {
    schema.getType match {
      case Schema.Type.UNION =>
        val subch = getPrincipalTypeFromUnion(schema)
        if (isPrimitiveType(subch.getType)) {
          getCatalystType(subch.getType)
        } else {
          subch.getType match {
            case Schema.Type.RECORD => transferAvroSchema(subch)
            case Schema.Type.ARRAY => transferAvroArraySchema(subch)
            case Schema.Type.MAP => transferAvroMapSchema(subch)
            case _ => throw new IllegalArgumentException("not support " + subch.toString + ", in array schema")
          }
        }
      case _ => throw new IllegalArgumentException("not union schema : " + schema.toString)
    }
  }

  def transferAvroMapSchema(schema: Schema)  :  DataType = {
    schema.getType match {
      case Schema.Type.MAP =>
        val valtype = schema.getValueType
        if (isPrimitiveType(valtype)) {
          MapType(StringType, getCatalystType(valtype.getType))
        } else {
          valtype.getType match {
            case Schema.Type.RECORD =>
              MapType((StringType), (transferAvroSchema(valtype)))
            case Schema.Type.ARRAY =>
              val arrType = transferAvroArraySchema(valtype)
              MapType((StringType), (arrType))
            case Schema.Type.MAP =>
              val mapType = transferAvroMapSchema(valtype)
              MapType((StringType), (mapType))
            case Schema.Type.UNION =>
              val uniType = transferAvroUnionSchema(valtype)
              MapType((StringType), (uniType))
            case _ => throw new IllegalArgumentException("not support " + valtype.toString + ", in map schema")
          }
        }

      case _ => throw new IllegalArgumentException("not map schema : " + schema.toString)
    }
  }

  def transferAvroField(field : Schema.Field) : (DataType, Boolean) = {
    val (answer : DataType, isNull : Boolean) = field.schema().getType match {
      case Schema.Type.INT => (IntegerType, false)
      case Schema.Type.DOUBLE => (DoubleType, false)
      case Schema.Type.FLOAT => (FloatType, false)
      case Schema.Type.LONG => (LongType, false)
      case Schema.Type.STRING => (StringType, false)
      case Schema.Type.BOOLEAN => (BooleanType, false)
      case Schema.Type.ENUM => (IntegerType, false)

      //TODO:
      case Schema.Type.BYTES => (ArrayType(BinaryType), false)
      case Schema.Type.MAP => (MapType((StringType), (transferAvroSchema(field.schema().getValueType))), false)

      //TODO: enum value should be transfered from int to string
      case Schema.Type.NULL => (null, null)

      //TODO: Array 嵌套 Union 不支持
      case Schema.Type.ARRAY => (transferAvroArraySchema(field.schema()), false)

      //TODO:
      case Schema.Type.FIXED => (null,null)
      case Schema.Type.UNION => (transferAvroUnionSchema(field.schema()), true)
      case Schema.Type.RECORD => (transferAvroSchema(field.schema()), false)
      case _ => throw new IllegalArgumentException("not support union schema with 3 or more items : ${schemaType.getName}")
    }

    (answer, isNull)
  }

  def transferAvroSchema(schema : Schema) : DataType = {
    if (isPrimitiveType(schema)) {
      getCatalystType(schema.getType)
    } else {
      schema.getType match {
        case Schema.Type.RECORD =>
          val fields = schema.getFields.asScala.map(field => {
            val (dataType, isNull) = transferAvroField(field)

            StructField(field.name(), dataType, isNull)
          })

          StructType(fields)
        case Schema.Type.ARRAY =>
          val arrType = transferAvroArraySchema(schema)
          arrType
        case Schema.Type.MAP =>
          val mapType = transferAvroMapSchema(schema)
          mapType
        case Schema.Type.UNION =>
          val uniTp = transferAvroUnionSchema(schema)
          uniTp
        case _ => throw new IllegalArgumentException("not support " + schema.toString + ", in map schema")
      }
    }
  }

  def transferAvroSchemaString(asvc : String) : DataType = {
    val schema = new Schema.Parser().parse(asvc)

    transferAvroSchema(schema)
  }
}
