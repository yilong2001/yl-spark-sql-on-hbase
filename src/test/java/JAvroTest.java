import com.example.spark.sql.util.ORMUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yilong on 2018/9/13.
 */
public class JAvroTest {
    static String userAvsc = "{\"namespace\": \"com.example.spark.demo.generated.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "     {\"name\": \"arr\", \"type\": {\"type\": \"array\", \"items\": \"string\"}} " +
            "]}";

    @Test
    public void testAvroSerde1() throws Exception {
        Schema schema = new Schema.Parser().parse(userAvsc);

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        List<String> arr = new ArrayList<String>();
        arr.add("1");
        arr.add("2");
        user2.put("arr", arr);

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
        datumWriter.write(user2, encoder);
        //datumWriter.write(user2, encoder);
        encoder.flush();
        out.close();

        /*
        *
        * */
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        //DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),null);
        GenericRecord result = datumReader.read(null,decoder);

        Assert.assertNotEquals(result, null);
        Assert.assertEquals(ORMUtil.getSqlFieldValue(result, "name").toString(), "Ben");
        Assert.assertEquals(ORMUtil.getSqlFieldValue(result, "arr[1]").toString(), "2");
        Assert.assertEquals(result.get("name").toString(), "Ben");
        Assert.assertEquals(((java.util.Collection)result.get("arr")).toArray()[1].toString(), "2");
    }

}
