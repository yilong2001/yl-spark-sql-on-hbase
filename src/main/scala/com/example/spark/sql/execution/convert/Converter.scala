package com.example.spark.sql.execution.convert

import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.example.spark.sql.util.StrUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
  * Created by yilong on 2018/7/9.
  */
trait ParentContainUpdater {
  def set(value: Any): Unit = ()
  def setBoolean(value: Boolean): Unit = set(value)
  def setByte(value: Byte): Unit = set(value)
  def setShort(value: Short): Unit = set(value)
  def setInt(value: Int): Unit = set(value)
  def setLong(value: Long): Unit = set(value)
  def setFloat(value: Float): Unit = set(value)
  def setDouble(value: Double): Unit = set(value)
  def setBinary(value: Byte) : Unit = set(value)
  def setString(value: String) : Unit = set(value)
}


class RowUpdater(row: InternalRow, ordinal: Int) extends ParentContainUpdater {
  override def set(value: Any): Unit = row(ordinal) = value
  override def setBoolean(value: Boolean): Unit = row.setBoolean(ordinal, value)
  override def setByte(value: Byte): Unit = row.setByte(ordinal, value)
  override def setShort(value: Short): Unit = row.setShort(ordinal, value)
  override def setInt(value: Int): Unit = row.setInt(ordinal, value)
  override def setLong(value: Long): Unit = row.setLong(ordinal, value)
  override def setDouble(value: Double): Unit = row.setDouble(ordinal, value)
  override def setFloat(value: Float): Unit = row.setFloat(ordinal, value)
  override def setBinary(value: Byte): Unit = row.setByte(ordinal, value)
  override def setString(value: String) : Unit = row.update(ordinal,value)
}

class SpecialRowUpdater() extends RowUpdater(null, 0) {
  var any : Any = null
  override def set(value: Any): Unit = {
    any = value
  }
}

class SpecialArrayRowUpdater() extends RowUpdater(null, 0) {
  var currentArray: ArrayBuffer[Any] = ArrayBuffer.empty

  override def set(value: Any): Unit = {
    currentArray += value
  }
}

trait ParentConverter {
  def convert(obj : Any)

  def avroUtf8ToString(obj : Any) : Any = {
    StrUtils.avroUtf8ToString(obj)
  }

  def toUTF8String(obj : Any) : UTF8String = {
    StrUtils.toUTF8String(obj)
  }
}

class PrimitiveConverter(rowUpdater: RowUpdater, dataType: DataType) extends ParentConverter{
  def convert(obj : Any) : Unit = {
    dataType match {
      case BooleanType => rowUpdater.setBoolean(obj.asInstanceOf[Boolean])
      case IntegerType => rowUpdater.setInt(obj.asInstanceOf[Int])
      case LongType => rowUpdater.setLong(obj.asInstanceOf[Long])
      case FloatType => rowUpdater.setFloat(obj.asInstanceOf[Float])
      case DoubleType => rowUpdater.setDouble(obj.asInstanceOf[Double])
      //case BinaryType => rowUpdater.setBinary(obj.asInstanceOf[Byte])
      case ByteType => rowUpdater.setByte(obj.asInstanceOf[Byte])
      case ShortType => rowUpdater.setShort(obj.asInstanceOf[Short])
      //case StringType => rowUpdater.setString(obj.asInstanceOf[String])
      case _ => throw new IllegalArgumentException(dataType.catalogString + " is wrong in PrimitiveConverter")
    }
  }
}

class StringConvert(rowUpdater: RowUpdater) extends ParentConverter {
  def convert(obj : Any) : Unit = {
    rowUpdater.set(toUTF8String(obj))
  }
}

class RepeatedPrimitiveConverter(rowUpdater: RowUpdater, dataType: ArrayType) extends ParentConverter{
  def convert(obj : Any) : Unit = {
    var currentArray: ArrayBuffer[Any] = ArrayBuffer.empty

    val collection = obj.asInstanceOf[java.util.Collection[Any]]
    collection.asScala.foreach(item => {
      currentArray += item
    })

    val arrData = new GenericArrayData(currentArray.toArray)
    rowUpdater.set(arrData)
  }
}

class RepeatedStringConverter(rowUpdater: RowUpdater, dataType: StringType) extends ParentConverter{
  def convert(obj : Any) : Unit = {
    var currentArray: ArrayBuffer[Any] = ArrayBuffer.empty

    val collection = obj.asInstanceOf[java.util.Collection[Any]]
    collection.asScala.foreach(item => {
      currentArray += toUTF8String(item)
    })

    val arrData = new GenericArrayData(currentArray.toArray)
    rowUpdater.set(arrData)
  }
}


abstract class BasicRepeatedRowConverter(rowUpdater: RowUpdater, dataType: ArrayType) extends ParentConverter{
  def convert(obj : Any) : Unit = {
    var currentArray: ArrayBuffer[Any] = ArrayBuffer.empty

    val collection = obj.asInstanceOf[java.util.Collection[Any]]
    val st = dataType.elementType.asInstanceOf[StructType]
    collection.asScala.foreach(item => {
      val newrowup = buildRowConverter(null, st); //new AvroRowConverter(null, st)
      newrowup.convert(item)

      //TODO: curRow ? currentRecord ?
      currentArray += newrowup.curRow
    })

    val arrData = new GenericArrayData(currentArray.toArray)

    rowUpdater.set(arrData)
  }

  def buildRowConverter(ru: RowUpdater, st: StructType) : BasicRowConverter

  def buildFieldConverter(ru: RowUpdater, dt: DataType) : ParentConverter
}

//,buildFieldConverter: (RowUpdater,DataType) => BasicFieldConverter
abstract class BasicMapConverter(rowUpdater: RowUpdater, dataType: MapType) extends ParentConverter{
  override def convert(obj: Any): Unit = {
    var currentKeys: ArrayBuffer[Any] = ArrayBuffer.empty
    var currentValues: ArrayBuffer[Any] = ArrayBuffer.empty

    val keyvalues = obj.asInstanceOf[java.util.Map[Any,Any]]

    val keyType = dataType.keyType
    val valtype = dataType.valueType

    keyvalues.entrySet().asScala.foreach(entry => {
      val keyup = convertKeyValue(rowUpdater, keyType, entry.getKey)

      val valup = convertKeyValue(rowUpdater, valtype, entry.getValue) //new AvroFieldConverter(null, valtype)

      //TODO: curRow ? currentRecord ?
      currentKeys += keyup
      currentValues += valup
    })

    rowUpdater.set(ArrayBasedMapData(currentKeys.toArray, currentValues.toArray))
  }

  def convertKeyValue(ru:RowUpdater, dt:DataType, item : Any) : Any
}


abstract class BasicFieldConverter(parentUp: RowUpdater, dataType: DataType) extends ParentConverter {
  val curRow = new SpecificInternalRow(Seq(dataType))

  val unsafeProjection = UnsafeProjection.create(Seq(dataType).toArray)

  def currentRecord: UnsafeRow = unsafeProjection(curRow)

  if (parentUp != null) {
    parentUp.set(curRow)
  }

  def convert(obj: Any): Unit = {
    val ru = new RowUpdater(curRow, 0)
    var convert = buildConverter(ru, dataType, "map["+obj.toString+"]")
    convert.convert(obj)
  }

  def buildConverter(ru: RowUpdater, dt: DataType, fieldName : String) : ParentConverter
}

abstract class BasicRowConverter(parentUp: RowUpdater, catalystType: StructType) extends ParentConverter {
  val curRow = new SpecificInternalRow(catalystType.map(_.dataType))

  val unsafeProjection = UnsafeProjection.create(catalystType)
  def currentRecord: UnsafeRow = unsafeProjection(curRow)

  if (parentUp != null) {
    parentUp.set(curRow)
  }

  def getObject(obj: Any, fieldname : String) : Any
  def buildConverter(ru: RowUpdater, dt: DataType, fieldName : String) : ParentConverter

  def convert(obj: Any): Unit = {
    catalystType.map(dt => {
      //val ele = nobj.get(dt.name)
      val ele = getObject(obj, dt.name)

      val ru = new RowUpdater(curRow, catalystType.fieldIndex(dt.name))
      var convert = buildConverter(ru, dt.dataType, dt.name)//AvroConverter.makeConverter(ru, dt.dataType, dt.name)

      convert.convert(ele)
    })
  }
}
