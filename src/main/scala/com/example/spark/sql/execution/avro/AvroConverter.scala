/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.spark.sql.execution.avro

import java.math.{BigDecimal, BigInteger}
import java.nio.charset.Charset
import java.nio.{ByteBuffer, ByteOrder}

import com.example.spark.sql.execution.convert._
import com.example.spark.sql.util.{ORMUtil, StrUtils}
import org.apache.avro.generic.GenericRecord

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.execution.datasources.parquet.{ParentContainerUpdater, ParquetPrimitiveConverter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._


class AvroRepeatedRowConverter(rowUpdater: RowUpdater, dataType: ArrayType) extends
  BasicRepeatedRowConverter(rowUpdater, dataType) {

  override def buildRowConverter(ru: RowUpdater, dt: StructType): BasicRowConverter = {
    new AvroRowConverter(ru, dt)
  }

  override def buildFieldConverter(ru: RowUpdater, dt: DataType) : ParentConverter = {
    AvroConverter.makeConverter(ru, dt, "nothing")
  }
}

class AvroFieldConverter(parentUp: RowUpdater, dataType: DataType) extends
  BasicFieldConverter(parentUp, dataType) {

  override def buildConverter(ru: RowUpdater, dt: DataType, fieldName : String): ParentConverter = {
    AvroConverter.makeConverter(ru, dt, fieldName)
  }
}

class AvroMapConverter(parentUp: RowUpdater, dataType: MapType) extends
  BasicMapConverter(parentUp, dataType) {

  override def convertKeyValue(ru: RowUpdater, dt: DataType, item : Any): Any = {
    AvroConverter.convertField(ru, dt, item)
  }
}

class AvroRowConverter(parentUp: RowUpdater, catalystType: StructType) extends
  BasicRowConverter(parentUp, catalystType) {
  override def getObject(obj: Any, fieldname: String): Any = {
    if (!(obj.isInstanceOf[GenericRecord])) {
      throw new IllegalArgumentException("obj is not GenericRecord")
    }
    val nobj = obj.asInstanceOf[GenericRecord]

    val ret = nobj.get(fieldname)

    ret
  }

  override def buildConverter(ru: RowUpdater, dt: DataType, fieldName: String): ParentConverter = {
    AvroConverter.makeConverter(ru, dt, fieldName)
  }
}

object AvroConverter {

  def convert(obj: Any, catalystType: StructType) : InternalRow = {
    val converter = new AvroRowConverter(null, catalystType)
    converter.convert(obj)

    //converter.currentRecord
    converter.curRow
  }

  def convertField(ru: RowUpdater, dt: DataType, item : Any) : Any = {
    val myRu = new SpecialRowUpdater()

    if (AvroConverter.isPrimitiveType(dt)) {
      item
    } else {
      dt match {
        case strt : StringType => StrUtils.toUTF8String(item)
        case datet : DateType => item
        case arrt : ArrayType => {
          val conv = AvroConverter.makeArrayConverter(myRu, arrt)
          conv.convert(item)

          myRu.any
        }
        case mapt : MapType => {
          val conv = new AvroMapConverter(myRu, mapt)
          conv.convert(item)

          myRu.any
        }
        case st : StructType => {
          val conv = new AvroRowConverter(ru, st)
          conv.convert(item)

          myRu.any
        }
      }
    }
  }

  def isPrimitiveType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | IntegerType | LongType | FloatType | DoubleType
           | BinaryType | ByteType | ShortType => true
      case _ => false
    }
  }

  def makeArrayConverter(ru:RowUpdater, t : ArrayType) : ParentConverter = {
    var convert : ParentConverter = null

    if (isPrimitiveType(t.elementType)) {
      convert = new RepeatedPrimitiveConverter(ru, t)
    } else if (t.elementType.isInstanceOf[StringType]) {
      convert = new RepeatedStringConverter(ru, t.elementType.asInstanceOf[StringType])//new AvroRepeatedRowConverter(ru, t)
    } else {
      convert = new AvroRepeatedRowConverter(ru, t)
    }

    convert
  }

  def makeConverter(ru:RowUpdater, dataType: DataType, fieldName: String) : ParentConverter = {
    var convert : ParentConverter = null
    dataType match {
      case BooleanType | IntegerType | LongType | FloatType | DoubleType
           | BinaryType | ByteType | ShortType =>
        convert = new PrimitiveConverter(ru, dataType)

      case StringType =>
        convert = new StringConvert(ru)

      case t: DecimalType =>
        throw new RuntimeException(
          s"Unable to create Avro converter for decimal type ${t.json} whose Avro type is " +
            s"${fieldName}.  Avro DECIMAL type can only be backed by INT32, INT64, " +
            "FIXED_LEN_BYTE_ARRAY, or BINARY.")

      case t: TimestampType =>
        throw new RuntimeException(
          s"Unable to create Avro converter for TimestampType type ${t.json} whose Thrift type is " +
            s"${fieldName}.  Avro TimestampType type can only be backed by INT64 ")

      case DateType =>
        convert = new PrimitiveConverter(ru, dataType) {
          override def convert(obj: Any): Unit = {
            dataType match {
              // DateType is not specialized in `SpecificMutableRow`, have to box it here.
              case t: DataType => ru.set(obj)
              case _ => super.convert(obj, dataType)
            }
          }
        }

      // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group nor
      // annotated by `LIST` or `MAP` should be interpreted as a required list of required
      // elements where the element type is the type of the field.
      case t: ArrayType =>
        convert = makeArrayConverter(ru, t)

      case t: MapType =>
        convert = new AvroMapConverter(ru, t)

      case t: StructType =>
        convert = new AvroRowConverter(ru, t)
      case t =>
        throw new RuntimeException(
          s"Unable to create Avro converter for data type ${t.json} " +
            s"whose Avro type is ${dataType}")
    }

    convert
  }
}

