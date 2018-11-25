package com.example.spark.sql.util

import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by yilong on 2018/7/16.
  */
object StrUtils {
  def avroUtf8ToString(obj : Any) : Any = {
    if (obj.isInstanceOf[org.apache.avro.util.Utf8]) {
      val u8 = obj.asInstanceOf[org.apache.avro.util.Utf8]
      new String(u8.getBytes)
    } else if (obj.isInstanceOf[String]) {
      obj.asInstanceOf[String]
    } else {
      obj
    }
  }

  def toUTF8String(obj : Any) : UTF8String = {
    val str : String = avroUtf8ToString(obj).toString

    val buffer = ByteBuffer.wrap(str.getBytes(Charset.forName("utf-8")))
    val offset = buffer.arrayOffset() + buffer.position()
    val numBytes = buffer.remaining()

    UTF8String.fromBytes(buffer.array(), offset, numBytes)
  }
}
