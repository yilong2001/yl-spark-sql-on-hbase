package com.example.spark.sql.execution.hbase

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}

/**
  * Created by yilong on 2018/6/24.
  */
object HBaseConn {
  var conn : Connection = null

  def getConnection(conf : Configuration): Connection = {
    val isConnReady = (conn != null && !conn.isClosed && !conn.isAborted)

    isConnReady match {
      case true => conn
      case _ => {
        var tmp : Connection = null
        AnyRef.synchronized({
          if ((conn != null && !conn.isClosed && !conn.isAborted)) conn
          else {
            if (conn != null) conn.close()
            try {
              conn = ConnectionFactory.createConnection(conf)
            } catch {
              case e : IOException => e.printStackTrace(); conn = null
            }

            conn
          }
        })
      }
    }
  }

}
