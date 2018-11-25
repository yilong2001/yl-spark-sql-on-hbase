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

package com.example.spark.sql.execution.hbase

import java.sql.Connection
import java.util.{Locale, Properties}

import com.example.spark.demo.impl.transfer.RowKeyTransfer
import com.example.spark.sql.execution.hbase.HBaseOptions.HBASE_TABLE_VALUE_CLASS_NAME
import com.example.spark.sql.util.{ClassUtilInScala, Serde}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

/**
 * Options for the hbase data source.
 */
class HBaseOptions(
    @transient private val parameters: CaseInsensitiveMap[String])
  extends Serializable {
  import HBaseOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  def this(table: String, parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters ++ Map(
      HBaseOptions.HBASE_TABLE_NAME -> table)))
  }

  /**
   * Returns a property with all options.
   */
  val asProperties: Properties = {
    val properties = new Properties()
    parameters.originalMap.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  /**
   * Returns a property with all options except Spark internal data source options like `url`,
   * `dbtable`, and `numPartition`. This should be used when invoking JDBC API like `Driver.connect`
   * because each DBMS vendor has its own property list for JDBC driver. See SPARK-17776.
   */
//  val asConnectionProperties: Configuration = {
//    val conf = HBaseConfiguration.create
//    conf
//  }

  // ------------------------------------------------------------
  // Required parameters
  // ------------------------------------------------------------
  require(parameters.isDefinedAt(HBASE_TABLE_NAME), s"Option '$HBASE_TABLE_NAME' is required.")
  require(parameters.isDefinedAt(HBASE_TABLE_FAMILY_NAME), s"Option '$HBASE_TABLE_FAMILY_NAME' is required.")

  // name of table
  val table = parameters(HBASE_TABLE_NAME)
  val family = parameters(HBASE_TABLE_FAMILY_NAME)

  require(parameters.isDefinedAt(HBASE_TABLE_ROWKEY_TRANSFER), s"Option '$HBASE_TABLE_ROWKEY_TRANSFER' is required.")

  val rowkeyTransfer : RowKeyTransfer = Serde.deSerialize(parameters(HBASE_TABLE_ROWKEY_TRANSFER))

  //val keyClassName = parameters.getOrElse(HBASE_TABLE_KEY_CLASS_NAME, "java.lang.String")
  //val valueClassName = parameters(HBASE_TABLE_VALUE_CLASS_NAME)

  //val valueClass = ClassUtilInScala.classForName(valueClassName)

  val tableAvsc = parameters(HBASE_TABLE_AVSC)

  // ------------------------------------------------------------
  // Optional parameters only for writing
  // TODO: don't support create/drop table
  // ------------------------------------------------------------
  // TODO: to reuse the existing partition parameters for those partition specific options
  val isolationLevel =
    parameters.getOrElse(HBASE_TXN_ISOLATION_LEVEL, "READ_UNCOMMITTED") match {
      case "NONE" => Connection.TRANSACTION_NONE
      case "READ_UNCOMMITTED" => Connection.TRANSACTION_READ_UNCOMMITTED
      case "READ_COMMITTED" => Connection.TRANSACTION_READ_COMMITTED
      case "REPEATABLE_READ" => Connection.TRANSACTION_REPEATABLE_READ
      case "SERIALIZABLE" => Connection.TRANSACTION_SERIALIZABLE
    }
}


object HBaseOptions {
  private val hbaseOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    hbaseOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val HBASE_TABLE_NAME = newOption("table")

  val HBASE_TABLE_FAMILY_NAME = newOption("tableFamilyClass")

  val HBASE_TABLE_ROWKEY_TRANSFER = newOption("tableRowkeyTransfer")

  val HBASE_TABLE_KEY_CLASS_NAME = newOption("tableKeyClass")
  val HBASE_TABLE_VALUE_CLASS_NAME = newOption("tableValueClass")

  val HBASE_ZK_QUORM = newOption("hbase.zookeeper.quorum")
  val HBASE_TXN_ISOLATION_LEVEL = newOption("isolationLevel")

  val HBASE_TABLE_AVSC = newOption("tableAvsc")
}

