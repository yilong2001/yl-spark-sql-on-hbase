package com.example.spark.demo.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.io.IOException;
import java.util.Map;

/**
 * Created by yilong on 2017/9/26.
 */
public class AdminClient {
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
            HTableDescriptor hTableDescriptor = conn.getAdmin().getTableDescriptor(TableName.valueOf("emp2"));
            HColumnDescriptor[] cds = hTableDescriptor.getColumnFamilies();
            for (HColumnDescriptor cd : cds) {
                System.out.println("*****************************************");
                System.out.println(cd.getNameAsString());
                System.out.println("*****************************************");
                for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry : cd.getValues().entrySet()) {
                    System.out.println("*****************************************");
                    System.out.println(new String(entry.getKey().copyBytes()));
                    System.out.println(new String(entry.getValue().copyBytes()));
                    System.out.println("*****************************************");
                }
            }

            RegionLocator rl = conn.getRegionLocator(TableName.valueOf("emp2"));
            for (byte[] key : rl.getEndKeys()) {
                System.out.println("rowkey : *****************************************");
                if (key == null || key.length == 0) {
                    System.out.println("NULL or len is 0");
                } else {
                    System.out.println(new String(key));
                }
                System.out.println("rowkey : *****************************************");
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
