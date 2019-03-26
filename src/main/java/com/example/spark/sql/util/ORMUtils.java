package com.example.spark.sql.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yilong on 2018/6/10.
 */
public class ORMUtils {
    private static Logger logger = Logger.getLogger(ORMUtils.class);

    public static Object getFieldObj(Object element, String fieldName) throws IllegalAccessException {
        Class clazz = element.getClass();
        Field[] fields = clazz.getDeclaredFields();
        for (Field fd : fields) {
            if (fd.getName().equals(fieldName)) {
                fd.setAccessible(true);
                try {
                    return fd.get(element);
                } catch (IllegalAccessException e) {
                    logger.error(e.getMessage(), e);
                    throw e;
                }
            }
        }

        return null;
    }

    public static Class getSqlFieldType(Object element, String sqlField) {
        if (element instanceof GenericRecord) {
            return getAvroFieldType(((GenericRecord)element).getSchema(), sqlField);
        }

        Object obj = getSqlFieldValue(element, sqlField);
        return obj.getClass();
    }

    public static Object getSqlFieldValue(Object element, String sqlField) {
        if (element instanceof GenericRecord) {
            return getAvroFieldValue(element, sqlField);
        }

        Object result = element;
        String[] sqlFields = sqlField.split("\\.");
        for (String fd : sqlFields) {
            if (isArrayField(fd)) {
                result = getArrayObject(result, fd);
            } else if (isMapField(fd)) {
                result = getMapObject(result, fd);
            } else {
                result = getSubParam(result, fd);
            }

            if (result == null) break;
        }

        return result;
    }

    private static Object getSubParam(Object element, String sqlField) {
        Class clazz = element.getClass();
        Field[] fields = clazz.getDeclaredFields();
        for (Field fd : fields) {
            if (fd.getName().equals(sqlField)) {
                logger.info(sqlField+" type : "+fd.getType().toGenericString());
                fd.setAccessible(true);
                try {
                    return fd.get(element);
                } catch (IllegalAccessException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        return null;
    }

    private static boolean isArrayField(String sqlField) {
        String regx = "^[a-z|A-Z|_]\\w*\\[\\d+\\]";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        return m.find();
    }

    private static boolean isMapField(String sqlField) {
        String regx = "^[a-z|A-Z|_]\\w*\\[['|\"]\\w+['|\"]\\]";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        return m.find();
    }

    private static Object getArrayObject(Object element, String sqlField) {
        String regx = "^([a-z|A-Z|_]\\w*)(\\[)(\\d+)(\\])";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        if (!m.find() || m.groupCount() != 4) {
            return null;
        }

        String field = m.group(1);
        int index = Integer.parseInt(m.group(3));
        Object fieldObj = getSubParam(element, field);
        if (fieldObj == null) {
            return null;
        }

        //for avro, array must be list
        if (fieldObj instanceof Collection) {
            Collection colobj = (Collection)fieldObj;
            if (colobj.size() <= index) {
                return null;
            }

            return colobj.toArray()[index];
        }

        //but, support array
        return Array.get(fieldObj, index);
    }

    private static Object getMapObject(Object element, String sqlField) {
        String regx = "^([a-z|A-Z|_]\\w*)(\\[)(['|\"])(\\w+)(['|\"])(\\])";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        if (!m.find() || m.groupCount() != 6) {
            return null;
        }

        String field = m.group(1);
        String key = m.group(4);
        Object fieldObj = getSubParam(element, field);
        if (fieldObj == null) {
            return null;
        }

        if (!(fieldObj instanceof Map)) {
            return null;
        }

        Map map = (Map)fieldObj;
        return map.get(key.toString());
    }

    private static Object getAvroFieldValue(Object element, String sqlField) {
        Object result = element;

        String[] sqlFields = sqlField.split("\\.");
        for (String fd : sqlFields) {
            if (isArrayField(fd)) {
                result = getAvroArrayObject(result, fd);
            } else if (isMapField(fd)) {
                result = getAvroMapObject(result, fd);
            } else {
                result = getAvroSubParam(result, fd);
            }

            if (result == null) break;
        }

        return result;
    }

    public static  Class getAvroFieldType(Schema schema, String sqlField) {
        String[] sqlFields = sqlField.split("\\.");
        Schema subschema = schema;

        for (String fd : sqlFields) {
            String subfield = fd.split("\\[")[0];

            Schema.Field field = subschema.getField(subfield);
            if (field == null) {
                return  null;
            }

            subschema = field.schema();
            if (subschema == null) {
                return null;
            }

            //TODO: UNION 类型 : 仅支持 type:["null","record"]
            if (subschema.getType().getName().equals(Schema.Type.UNION.getName())) {
                List<Schema> subtypes = subschema.getTypes();
                if (subtypes.size() != 2) {
                    return null;
                }

                subschema = subtypes.get(0);
                if (subtypes.get(0).getName().equals(Schema.Type.NULL.getName())) {
                    subschema = subtypes.get(1);
                }
            }

            if (subschema.getType().getName().equals(Schema.Type.MAP.getName())) {
                subschema = subschema.getValueType();
            }

            if (subschema.getType().getName().equals(Schema.Type.ARRAY.getName())) {
                subschema = subschema.getElementType();
            }

            if (subschema.getType().getName().equals(Schema.Type.INT.getName())) return Integer.class;
            if (subschema.getType().getName().equals(Schema.Type.DOUBLE.getName())) return Double.class;
            if (subschema.getType().getName().equals(Schema.Type.LONG.getName())) return Long.class;
            if (subschema.getType().getName().equals(Schema.Type.STRING.getName())) return String.class;
            if (subschema.getType().getName().equals(Schema.Type.BOOLEAN.getName())) return Boolean.class;
            if (subschema.getType().getName().equals(Schema.Type.BYTES.getName())) return Byte[].class;
            if (subschema.getType().getName().equals(Schema.Type.ENUM.getName())) return Enum.class;
        }

        return null;
    }

    private static Object getAvroSubParam(Object element, String field) {
        if (element instanceof GenericRecord) {
            return ((GenericRecord)element).get(field);
        }

        if (element instanceof Map) {
            return ((Map)element).get(field);
        }

        return null;
    }

    private static Object getAvroArrayObject(Object element, String sqlField) {
        String regx = "^([a-z|A-Z|_]\\w*)(\\[)(\\d+)(\\])";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        if (!m.find() || m.groupCount() != 4) {
            return null;
        }

        String field = m.group(1);
        int index = Integer.parseInt(m.group(3));
        Object fieldObj = getAvroSubParam(element, field);
        if (fieldObj == null) {
            return null;
        }

        //for avro, array must be list
        return ((java.util.Collection)fieldObj).toArray()[index];
    }

    private static Object getAvroMapObject(Object element, String sqlField) {
        String regx = "^([a-z|A-Z|_]\\w*)(\\[)(['|\"])(\\w+)(['|\"])(\\])";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        if (!m.find() || m.groupCount() != 6) {
            return null;
        }

        String field = m.group(1);
        String key = m.group(4);
        Object fieldObj = getAvroSubParam(element, field);
        if (fieldObj == null) {
            return null;
        }

        if (!(fieldObj instanceof Map)) {
            return null;
        }

        Map map = (Map)fieldObj;
        return map.get(key.toString());
    }

    static class TestSub {
        public int x;
        public String y;
        public Map<String,String> c;
        public TestSub(int x, String y) {
            this.x = x;
            this.y = y;
        }
    }

    static class TestObj {
        public int x;
        public String y;
        public TestSub[] tss;
        public Map<String, TestSub> mts;
    }

    public static void testComposeFieldAnalysis() {
        System.out.println(isMapField("c['a1']"));

        //System.exit(1);
        TestSub ts1 = new TestSub(1, "a1");
        ts1.c = new HashMap<>();
        ts1.c.put("a1","ab1");
        TestSub ts2 = new TestSub(2, "a2");
        ts2.c = new HashMap<>();
        ts2.c.put("a1","bb1");

        TestSub[] tss = {ts1, ts2};
        TestObj to = new TestObj();
        to.x = 1;
        to.y = "c1";
        to.tss = tss;
        to.mts = new HashMap<>();
        to.mts.put("d1", ts1);

        String sql0 = "tss[0].x";
        String sql1 = "tss[0].c['a1']";
        String sql2 = "mts['d1'].c['a1']";

        Object res0 = getSqlFieldValue( to, sql0);
        System.out.println(res0.toString());

        Object res1 = getSqlFieldValue( to, sql1);
        System.out.println(res1.toString());

        Object res2 = getSqlFieldValue( to, sql2);
        System.out.println(res2.toString());
    }

    public static void  main(String[] args) throws IllegalAccessException {
        //testAvroSchema();
    }
}
