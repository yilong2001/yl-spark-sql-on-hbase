import java.io.{ByteArrayOutputStream, IOException}
import java.util
import java.util.{ArrayList, HashMap, List}

import MYSCH2.{TAB2, columns}
import com.example.spark.demo.impl.cmp.{EqualCompator, ValueCondition}
import com.example.spark.demo.impl.transfer.{RowKeyTransfer, ValueTransfer}
import com.example.spark.hbase.filter.SingleAvroFilter
import com.example.spark.sql.execution.avro.{AvroConverter, AvroSchema}
import com.example.spark.sql.execution.hbase.{HBaseOptions, HBaseRDD, HBaseRelationProvider}
import com.example.spark.sql.util.{ORMUtil, Serde}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io._
import org.apache.hadoop.hbase.client.{HTable, ResultScanner, Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.{And, EqualTo, Filter, Or}
import org.apache.spark.sql.types.StructType


/**
  * Created by yilong on 2018/6/25.
  */
object AvroTestUtils {

  def testAvroTransferSchema_1() : Unit = {
    val userAvsc = "{\"namespace\": \"com.example.spark.demo.generated.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"User\",\n" + " \"fields\": [\n" + "     {\"name\": \"name\", \"type\": \"string\"},\n" + "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}, \n" + "     {\"name\": \"colr\",\"type\": {\"type\": \"enum\", \"name\": \"Color\", \"symbols\" : [\"WHITE\", \"RED\", \"GREEN\"] } }, \n" + "     {\"name\": \"arr\", \"type\": {\"type\": \"array\", \"items\": \"string\"}} " + "]}"
    val schema = new Schema.Parser().parse(userAvsc)
    val st = AvroSchema.transferAvroSchema(schema)
    println(st)
  }

  def testAvroTransferSchema_2() : Unit = {
    val avsc = "{\"type\": \"array\",\n \"items\":{\n   \"type\":\"record\",\n   \"name\":\"products\",\n     \"fields\": [\n         {\"name\": \"product_seris\", \"type\": \"string\"},\n         {\"name\": \"product_name\", \"type\": \"string\"},\n         {\"name\": \"prices\", \"type\":\n            {\"type\": \"array\",\n                \"items\":{\n                    \"type\":\"record\",\n                    \"name\" : \"price\",\n                    \"fields\":[\n                      {\"name\":\"model\",\"type\":\"string\"},\n                      {\"name\":\"price\",\"type\":\"float\"}\n                    ]}\n              }\n         }\n     ]\n }\n}"
    val schema = new Schema.Parser().parse(avsc)
    val st = AvroSchema.transferAvroArraySchema(schema)
    println(st)
  }

  def testAvroTransferSchema_3() : Unit = {
    val avsc = "{\n  \"type\" : \"record\",\n  \"name\" : \"TAB2\",\n  \"namespace\" : \"MYSCH2\",\n  \"fields\" : [ {\n    \"name\" : \"table\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_type\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_ts\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"current_ts\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"pos\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"primary_keys\",\n    \"type\" : {\n      \"type\" : \"array\",\n      \"items\" : \"string\"\n    }\n  }, {\n    \"name\" : \"tokens\",\n    \"type\" : {\n      \"type\" : \"map\",\n      \"values\" : \"string\"\n    },\n    \"default\" : { }\n  }, {\n    \"name\" : \"before\",\n    \"type\" : [ \"null\", {\n      \"type\" : \"record\",\n      \"name\" : \"columns\",\n      \"fields\" : [ {\n        \"name\" : \"ID\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"ID_isMissing\",\n        \"type\" : \"boolean\"\n      }, {\n        \"name\" : \"NAME\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"NAME_isMissing\",\n        \"type\" : \"boolean\"\n      } ]\n    } ],\n    \"default\" : null\n  }, {\n    \"name\" : \"after\",\n    \"type\" : [ \"null\", \"columns\" ],\n    \"default\" : null\n  } ]\n}"
    val schema = new Schema.Parser().parse(avsc)
    val st = AvroSchema.transferAvroSchema(schema)
    println(st)
  }

  def testAvroConverter_1() : Unit = {
    val avsc = "{\n  \"type\" : \"record\",\n  \"name\" : \"TAB2\"," +
      "\n  \"namespace\" : \"MYSCH2\"," +
      "\n  \"fields\" : [ {\n    \"name\" : \"table\",\n    \"type\" : \"string\"\n  }, " +
      "{\n    \"name\" : \"op_type\",\n    \"type\" : \"string\"\n  }, " +
      "{\n    \"name\" : \"op_ts\",\n    \"type\" : \"string\"\n  }, " +
      "{\n    \"name\" : \"current_ts\",\n    \"type\" : \"string\"\n  }, " +
      "{\n    \"name\" : \"pos\",\n    \"type\" : \"string\"\n  }, " +
      "{\n    \"name\" : \"primary_keys\"," +
      "\n    \"type\" : {\n      \"type\" : \"array\",\n      \"items\" : \"string\"\n    }\n  }, " +
      "{\n    \"name\" : \"tokens\",\n    \"type\" : {\n      \"type\" : \"map\",\n      \"values\" : \"string\"\n    },\n    \"default\" : { }\n  }, {\n    \"name\" : \"before\",\n    \"type\" : [ \"null\", {\n      \"type\" : \"record\",\n      \"name\" : \"columns\",\n      \"fields\" : [ {\n        \"name\" : \"ID\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"ID_isMissing\",\n        \"type\" : \"boolean\"\n      }, {\n        \"name\" : \"NAME\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"NAME_isMissing\",\n        \"type\" : \"boolean\"\n      } ]\n    } ],\n    \"default\" : null\n  }, {\n    \"name\" : \"after\",\n    \"type\" : [ \"null\", \"columns\" ],\n    \"default\" : null\n  } ]\n}"

    val schema = new Schema.Parser().parse(avsc)
    val tab2 = new TAB2()

    val clbefore = new columns()
    clbefore.setID("id1")
    clbefore.setNAME("name1")

    val clafter = new columns()
    clafter.setID("id2")
    clafter.setNAME("name2")

    tab2.setAfter(clafter)
    tab2.setBefore(clbefore)
    tab2.setCurrentTs("ts1")
    tab2.setOpTs("ts1")
    tab2.setOpType("I")
    tab2.setPos("pos")
    val list = new util.ArrayList[CharSequence]()
    list.add("pk1")
    list.add("pk2")
    tab2.setPrimaryKeys(list)
    tab2.setTable("table1")

    val map = new util.HashMap[CharSequence,CharSequence]()
    map.put("k1","v1")
    map.put("k2","v2")
    tab2.setTokens(map)

    //tab2.setTokens(null)

    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    //DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

    var outputStream : ByteArrayOutputStream = null
    try {
      outputStream = new ByteArrayOutputStream(avsc.length)
      outputStream.write(avsc.getBytes)
    } catch {
      case e: Exception => e.printStackTrace()
    }

    val out = new ByteArrayOutputStream
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    datumWriter.write(tab2, encoder)
    //datumWriter.write(user2, encoder);
    encoder.flush()
    out.close()

    val datumReader = new GenericDatumReader[GenericRecord](schema)
    //DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
    val decoder = DecoderFactory.get.binaryDecoder(out.toByteArray, null)
    val result = datumReader.read(null, decoder)

    val st = AvroSchema.transferAvroSchema(schema)

    val row = AvroConverter.convert(result, st.asInstanceOf[StructType])

    println(row)
  }

  def testAvroFilter() = {
    val avsc = "{\n  \"type\" : \"record\",\n  \"name\" : \"TAB2\",\n  \"namespace\" : \"MYSCH2\",\n  \"fields\" : [ {\n    \"name\" : \"table\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_type\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_ts\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"current_ts\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"pos\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"primary_keys\",\n    \"type\" : {\n      \"type\" : \"array\",\n      \"items\" : \"string\"\n    }\n  }, {\n    \"name\" : \"tokens\",\n    \"type\" : {\n      \"type\" : \"map\",\n      \"values\" : \"string\"\n    },\n    \"default\" : { }\n  }, {\n    \"name\" : \"before\",\n    \"type\" : [ \"null\", {\n      \"type\" : \"record\",\n      \"name\" : \"columns\",\n      \"fields\" : [ {\n        \"name\" : \"ID\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"ID_isMissing\",\n        \"type\" : \"boolean\"\n      }, {\n        \"name\" : \"NAME\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"NAME_isMissing\",\n        \"type\" : \"boolean\"\n      } ]\n    } ],\n    \"default\" : null\n  }, {\n    \"name\" : \"after\",\n    \"type\" : [ \"null\", \"columns\" ],\n    \"default\" : null\n  } ]\n}"
    val rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator("row5".getBytes))
    val valueCondition = new EqualCompator("node", "localhost:0")
    val avrofilter : SingleAvroFilter = new SingleAvroFilter("f".getBytes, rowFilter, valueCondition, avsc)

    val sers = avrofilter.toByteArray
    val avrofilter1 = SingleAvroFilter.parseFrom(sers)

    System.out.println(avrofilter1.getAvsc)
    System.out.println(avrofilter1.getRowfilter)
  }

  def buildAvroObject: Any = {
    val avsc = "{\n  \"type\" : \"record\",\n  \"name\" : \"TAB2\"," +
      "\n  \"namespace\" : \"MYSCH2\"," +
      "\n  \"fields\" : [ {\n    \"name\" : \"table\",\n    \"type\" : \"string\"\n  }, " +
      "{\n    \"name\" : \"op_type\",\n    \"type\" : \"string\"\n  }, " +
      "{\n    \"name\" : \"op_ts\",\n    \"type\" : \"string\"\n  }, " +
      "{\n    \"name\" : \"current_ts\",\n    \"type\" : \"string\"\n  }, " +
      "{\n    \"name\" : \"pos\",\n    \"type\" : \"string\"\n  }, " +
      "{\n    \"name\" : \"primary_keys\"," +
      "\n    \"type\" : {\n      \"type\" : \"array\",\n      \"items\" : \"string\"\n    }\n  }, " +
      "{\n    \"name\" : \"tokens\",\n    \"type\" : {\n      \"type\" : \"map\",\n      \"values\" : \"string\"\n    },\n    \"default\" : { }\n  }, {\n    \"name\" : \"before\",\n    \"type\" : [ \"null\", {\n      \"type\" : \"record\",\n      \"name\" : \"columns\",\n      \"fields\" : [ {\n        \"name\" : \"ID\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"ID_isMissing\",\n        \"type\" : \"boolean\"\n      }, {\n        \"name\" : \"NAME\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"NAME_isMissing\",\n        \"type\" : \"boolean\"\n      } ]\n    } ],\n    \"default\" : null\n  }, {\n    \"name\" : \"after\",\n    \"type\" : [ \"null\", \"columns\" ],\n    \"default\" : null\n  } ]\n}"
    val schema = new Schema.Parser().parse(avsc)
    val user2 = new GenericData.Record(schema)
    user2.put("table", "table1")
    user2.put("op_type", "insert")
    user2.put("op_ts", "000000")
    user2.put("pos", "1")
    val arr = new util.ArrayList[String]
    arr.add("1")
    arr.add("2")
    user2.put("primary_keys", arr)
    val map = new util.HashMap[CharSequence, CharSequence]
    map.put("k1", "v1")
    map.put("k2", "v2")
    user2.put("tokens", map)
    user2
  }

  def testAvroSchema(): Unit = {
    val element = buildAvroObject
    val field0 = "table"
    val field1 = "tokens['k1']"
    val field2 = "primary_keys[0]"
    val field3 = "after.NAME"
    val field = field3
    val schema = element.asInstanceOf[GenericRecord].getSchema
    println(ORMUtil.getAvroFieldType(schema, field))
  }

  def testAvroHBaseFilter(): Unit = {
    val avsc = "{\n  \"type\" : \"record\",\n  \"name\" : \"TAB2\",\n  \"namespace\" : \"MYSCH2\",\n  \"fields\" : [ {\n    \"name\" : \"table\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_type\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"op_ts\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"current_ts\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"pos\",\n    \"type\" : \"string\"\n  }, {\n    \"name\" : \"primary_keys\",\n    \"type\" : {\n      \"type\" : \"array\",\n      \"items\" : \"string\"\n    }\n  }, {\n    \"name\" : \"tokens\",\n    \"type\" : {\n      \"type\" : \"map\",\n      \"values\" : \"string\"\n    },\n    \"default\" : { }\n  }, {\n    \"name\" : \"before\",\n    \"type\" : [ \"null\", {\n      \"type\" : \"record\",\n      \"name\" : \"columns\",\n      \"fields\" : [ {\n        \"name\" : \"ID\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"ID_isMissing\",\n        \"type\" : \"boolean\"\n      }, {\n        \"name\" : \"NAME\",\n        \"type\" : [ \"null\", \"string\" ],\n        \"default\" : null\n      }, {\n        \"name\" : \"NAME_isMissing\",\n        \"type\" : \"boolean\"\n      } ]\n    } ],\n    \"default\" : null\n  }, {\n    \"name\" : \"after\",\n    \"type\" : [ \"null\", \"columns\" ],\n    \"default\" : null\n  } ]\n}"
    val schema = new Schema.Parser().parse(avsc)
    val rowKeyTransfer = new ValueTransfer("table")
    val filter1 =  And(EqualTo("table", "table1"), EqualTo("primary_keys[0]", "pk11"))
    //
    val arr = Array[Filter](filter1)
    var (rowFilter, valueCondition) = HBaseRDD.buildRowkeyAndValueFilter(rowKeyTransfer, arr)

    val avrofilter = new SingleAvroFilter("f".getBytes, rowFilter, valueCondition, avsc)

    val f1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("table1")))

    val f2 = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    f2.asInstanceOf[FilterList].addFilter(f1)

    rowFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    rowFilter.asInstanceOf[FilterList].addFilter(f2)

    val fba = rowFilter.toByteArray

    val newfilter = FilterList.parseFrom(fba)

    assert(newfilter != null)

    val avrofba = avrofilter.toByteArray

    val newavrofilter = SingleAvroFilter.parseFrom(avrofba)

    assert(newavrofilter.getAvsc.equals(avrofilter.getAvsc.toString))
  }

  def main(args: Array[String]): Unit = {
    //testAvroTransferSchema_1()
    //testAvroTransferSchema_2()
    //testAvroTransferSchema_3()
    testAvroConverter_1()
    //testAvroFilter()
    //testAvroSchema()
    //testAvroHBaseFilter()
  }
}
