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

import com.example.spark.sql.util.HBaseUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}

class HBaseRelationProvider extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {

  override def shortName(): String = "com.example.spark.sql.execution.hbase.HBaseRelationProvider"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {

    val hbaseOptions = new HBaseOptions(parameters)

    val hbasePartitionInfo = HBasePartitioningInfo(HBaseUtils.getSplits(hbaseOptions))

    val parts = HBaseRelation.columnPartition(hbasePartitionInfo)

    HBaseRelation(parts, hbaseOptions)(sqlContext.sparkSession)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    val hbaseOptions = new HBaseOptions(parameters)

    //TODO: create table && save df into hbase tables

    createRelation(sqlContext, parameters)
  }
}
