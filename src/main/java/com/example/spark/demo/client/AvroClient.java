package com.example.spark.demo.client;

import MYSCH2.TAB2;
import MYSCH2.columns;
import com.example.spark.demo.impl.cmp.EqualCompator;
import com.example.spark.demo.impl.cmp.ValueCondition;
import com.example.spark.demo.impl.transfer.RowKeyTransfer;
import com.example.spark.demo.impl.transfer.ValueTransfer;
import com.example.spark.hbase.coprocessor.SingleSqlCountClient;
import com.example.spark.hbase.filter.SingleAvroFilter;
import com.example.spark.sql.util.ORMUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RowFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by yilong on 2018/7/9.
 */
public class AvroClient {
    static String userAvsc = "{\"namespace\": \"com.example.spark.demo.generated.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "     {\"name\": \"arr\", \"type\": {\"type\": \"array\", \"items\": \"string\"}} " +
            "]}";

    static String tab2Avsc = "{\n  \"type\" : \"record\",\n  \"name\" : \"TAB2\"," +
            "\n  \"namespace\" : \"MYSCH2\",\n  \"fields\" : [ {\n    \"name\" : \"table\"," +
            "\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_type\"," +
            "\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_ts\"," +
            "\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"current_ts\"," +
            "\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"pos\"," +
            "\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"primary_keys\"," +
            "\n    \"type\" : {\n      \"type\" : \"array\"," +
            "\n      \"items\" : \"string\"\n    }\n  }, " +
            "{\n    \"name\" : \"tokens\",\n    \"type\" : {\n      \"type\" : \"map\"," +
            "\n      \"values\" : \"string\"\n    },\n    \"default\" : { }\n  }, " +
            "{\n    \"name\" : \"before\",\n    \"type\" : [ \"null\", " +
            "{\n      \"type\" : \"record\",\n      \"name\" : \"columns\"," +
            "\n      \"fields\" : [ {\n        \"name\" : \"ID\"," +
            "\n        \"type\" : [ \"null\", \"string\" ]," +
            "\n        \"default\" : null\n      }, {\n        \"name\" : \"ID_isMissing\"," +
            "\n        \"type\" : \"boolean\"\n      }, {\n        \"name\" : \"NAME\"," +
            "\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, " +
            "{\n        \"name\" : \"NAME_isMissing\",\n        \"type\" : \"boolean\"\n      } ]\n    } ],\n    \"default\" : null\n  }, {\n    \"name\" : \"after\",\n    \"type\" : [ \"null\", \"columns\" ],\n    \"default\" : null\n  } ]\n}";

    private static void saveUserInfo(HTable table, int id) throws Exception {
        Schema schema = new Schema.Parser().parse(userAvsc);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "table"+id);
        user2.put("favorite_number", id);
        List<String> arr = new ArrayList<String>();
        arr.add("a1"+id);
        arr.add("a2"+id);
        user2.put("arr", arr);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        datumWriter.write(user2, encoder);
        //datumWriter.write(user2, encoder);
        encoder.flush();
        out.close();

        Put put = new Put(("table"+id).getBytes());
        put.addColumn("f".getBytes(), "f".getBytes(), out.toByteArray());

        table.put(put);
    }

    private static void saveTab2Info(HTable table,int id) throws Exception {
        Schema schema = new Schema.Parser().parse(tab2Avsc);
        TAB2 tab2 = new TAB2();

        columns clbefore = new columns();
        clbefore.setID("id2");
        clbefore.setNAME("name2");

        columns clafter = new columns();
        clafter.setID("id2");
        clafter.setNAME("name21");

        tab2.setAfter(clafter);
        tab2.setBefore(clbefore);
        tab2.setCurrentTs("ts1");
        tab2.setOpTs("ts1");
        tab2.setOpType("I");
        tab2.setPos("pos");
        List list = new ArrayList<CharSequence>();
        list.add("pk1"+id);
        list.add("pk2"+id);
        tab2.setPrimaryKeys(list);
        tab2.setTable("table"+id);
        Map map = new HashMap<CharSequence,CharSequence>();
        map.put("k1","v1"+id);
        map.put("k2","v2"+id);
        tab2.setTokens(map);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        //DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream outputStream = null;
        try {
            outputStream = new ByteArrayOutputStream(userAvsc.length());
            outputStream.write(userAvsc.getBytes());
        } catch (Exception e) {

        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        datumWriter.write(tab2, encoder);
        //datumWriter.write(user2, encoder);
        encoder.flush();
        out.close();

        Put put = new Put(("table"+id).getBytes());
        put.addColumn("f".getBytes(), "f".getBytes(), out.toByteArray());

        table.put(put);
    }

    public static void QueryAll(HTable table) {
        String avsc = "{\n  \"type\" : \"record\",\n  \"name\" : \"TAB2\",\n  \"namespace\" : \"MYSCH2\",\n  \"fields\" : [ {\n    \"name\" : \"table\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_type\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_ts\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"current_ts\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"pos\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"primary_keys\",\n    \"type\" : {\n      \"type\" : \"array\",\n      \"items\" : \"string\"\n    }\n  }, {\n    \"name\" : \"tokens\",\n    \"type\" : {\n      \"type\" : \"map\",\n      \"values\" : \"string\"\n    },\n    \"default\" : { }\n  }, {\n    \"name\" : \"before\",\n    \"type\" : [ \"null\", {\n      \"type\" : \"record\",\n      \"name\" : \"columns\",\n      \"fields\" : [ {\n        \"name\" : \"ID\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"ID_isMissing\",\n        \"type\" : \"boolean\"\n      }, {\n        \"name\" : \"NAME\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"NAME_isMissing\",\n        \"type\" : \"boolean\"\n      } ]\n    } ],\n    \"default\" : null\n  }, {\n    \"name\" : \"after\",\n    \"type\" : [ \"null\", \"columns\" ],\n    \"default\" : null\n  } ]\n}";
        Schema schema = new Schema.Parser().parse(avsc);
        //ValueCondition valueCondition = new EqualCompator("table", "table1");

        RowKeyTransfer rowKeyTransfer = new ValueTransfer("table");
        org.apache.spark.sql.sources.Filter filter1 = new org.apache.spark.sql.sources.And(
                new org.apache.spark.sql.sources.EqualTo("table", "table1"),
                new org.apache.spark.sql.sources.EqualTo("primary_keys[0]", "pk11"));

        //
        org.apache.spark.sql.sources.Filter arr[] = new org.apache.spark.sql.sources.Filter[]{filter1};
        scala.Tuple2 tuple2 = null; //com.example.spark.sql.execution.hbase.HBaseRDD.buildRowkeyAndValueFilter(rowKeyTransfer, arr);

        ValueCondition valueCondition = (ValueCondition)tuple2._2();
        org.apache.hadoop.hbase.filter.Filter rowFilter = (org.apache.hadoop.hbase.filter.Filter)tuple2._1();

        try {
            Scan s = new Scan();
            //RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
            //        new BinaryPrefixComparator("row5".getBytes()));
            //RowFilter rowFilter = null;
            SingleAvroFilter filter = new SingleAvroFilter("f".getBytes(),
                    rowFilter, valueCondition, avsc);
            //s.setFilter(filter);

            SingleSqlCountClient singleSqlCountClient = new SingleSqlCountClient("avro_t1", "f", "f", filter);
            System.out.println("count : "+ singleSqlCountClient.doCount().count + ", " + singleSqlCountClient.doCount().byteSize);

            ResultScanner rs = table.getScanner(s);

            //System.out.println(rs.next());

            for (Result r : rs) {
                System.out.println(new String(r.listCells().get(0).getFamilyArray()));
                System.out.println(new String(r.listCells().get(0).getQualifierArray()));
                System.out.println(new String(r.listCells().get(0).getFamily()));
                System.out.println(new String(r.listCells().get(0).getQualifier()));
                System.out.println(new String(r.listCells().get(0).getValue()));
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (org.apache.hadoop.hbase.KeyValue keyValue : r.raw()) {
                    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
                    //DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
                    Decoder decoder = DecoderFactory.get()
                            .binaryDecoder(keyValue.getValue(),null);
                    GenericRecord result = datumReader.read(null,decoder);
                    System.out.println(result);
                    System.out.println(new String(keyValue.getValue()));
                    System.out.println(valueCondition.isMatched(result));

                    rowFilter.reset();

                    System.out.println("rowkey filter : "+rowFilter.filterRowKey(r.getRow(), 0, r.getRow().length));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void hbaseOp() throws Exception {
        Configuration conf= HBaseConfiguration.create();
        Connection conn=null;
        //Table table  =null;
        HTable table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            //table =conn.getTable(TableName.valueOf("thrift_test1"));

            int id = 6;

            table = new HTable(conf, TableName.valueOf("avro_t1"));
            //saveTab2Info(table,id);

            //table = new HTable(conf, TableName.valueOf("avro_t2"));
            //saveUserInfo(table,id);

            //getInfo(table);

            QueryAll(table);
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

            if(table!=null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }finally{
                    table=null;
                }
            }
        }
    }

    public static void  main(String[] args) throws Exception {
        //Schema schema1 = new Schema.Parser().parse(new File("user.avsc"));
        //Schema schema = new Schema.Parser().parse(userAvsc);
        //showSchema(schema);
        //serde();
        hbaseOp();
    }
}
