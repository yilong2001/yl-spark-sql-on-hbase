package com.example.spark.demo.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by yilong on 2017/9/26.
 */
public class OriginalClient {
    public static Configuration kerberosInit(){
        Configuration conf = HBaseConfiguration.create();
        return conf;
    }

    public static void main(String[] args) {
        //先过kerberos
        Configuration conf= kerberosInit();
        Connection conn=null;
        Table tableInterface =null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            tableInterface =conn.getTable(TableName.valueOf("example"));
            Scan s = (new Scan()).setMaxVersions().setBatch(2).setCaching(1000);
            Result result = tableInterface.get(new Get("row1".getBytes()));
            String family=null;
            String qualifier;
            for(Cell cell:result.listCells()){
                family=Bytes.toString(CellUtil.cloneFamily(cell));
                System.out.println("****** : "+family);
                qualifier=Bytes.toString(CellUtil.cloneQualifier(cell));
                System.out.println("****** : "+qualifier);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(conn!=null) {
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    conn = null;
                }
            }

            if(tableInterface!=null) {
                try {
                    tableInterface.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }finally{
                    tableInterface=null;
                }
            }
        }
    }
}
