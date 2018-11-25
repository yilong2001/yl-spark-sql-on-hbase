package com.example.spark.demo.client

import java.io.IOException

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}


/**
  * Created by yilong on 2018/7/4.
  */
trait SpecializedGetters {
  def getBoolean(ordinal: Int): Boolean
  def setBoolean(value: Boolean) : Unit = {}

  def getInt(ordinal: Int): Int
}

class InternalRow extends SpecializedGetters with Serializable {
  override def getBoolean(ordinal: Int): Boolean = false

  override def getInt(ordinal: Int): Int = 1
}

class Predicate(id : Int) {
  def eval(r: InternalRow): Boolean = {
     id > 1
  }

  /**
    * Initializes internal states given the current partition index.
    * This is used by nondeterministic expressions to set initial states.
    * The default implementation does nothing.
    */
  def initialize(partitionIndex: Int): Unit = {}
}

object ScalaClient {
  def main(args: Array[String]): Unit = {
    val v1 = new Some[Int](1)

    val condfun : (InternalRow) => Boolean = {
      v1.map { id =>
        new Predicate(id).eval _
      }.getOrElse {
        ( (rr : InternalRow) => true )
      }
    }

    condfun(new InternalRow)
  }

  def main1(args: Array[String]): Unit = {
    var conn: Connection = null
    val tableName = "hbase1"
    try {
      conn = ConnectionFactory.createConnection(HBaseConfiguration.create())

      val rl = conn.getRegionLocator(TableName.valueOf(tableName))
      var id = 0
      if (rl == null || rl.getEndKeys == null || rl.getEndKeys.length == 0) {
        println(tableName + ", get region locator, but NULL. ")
        throw new IOException(tableName + ", get region locator, but NULL (or endkeys is empty). ")
      }

      for (key : Array[Byte] <- rl.getEndKeys) {
        println("rowkey : *****************************************")
        if (key == null || key.length == 0) {
          //TODO:
        }
        else {
          println(new String(key))
          id = id + 1
        }
      }

    } catch {
      case e: IOException => {
        e.printStackTrace()
      }
    } finally {
      if (conn != null) {
        try
          conn.close()
        catch {
          case e: IOException => e.printStackTrace()
        } finally conn = null
      }
    }
  }
}
