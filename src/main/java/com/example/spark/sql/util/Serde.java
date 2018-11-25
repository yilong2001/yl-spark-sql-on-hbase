package com.example.spark.sql.util;


import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yilong on 2017/7/31.
 */
public class Serde {
    private static Logger logger = Logger.getLogger(Serde.class);

    public static <T extends Serializable> String serialize(T obj)  throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(2 * 1024);
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);

        oos.close();

        return Base64.encodeBase64String(baos.toByteArray());
    }

    public static <T extends Serializable> T deSerialize(String str) throws Exception {
        byte[] bts = Base64.decodeBase64(str);
        ByteArrayInputStream bais = new ByteArrayInputStream(bts);
        ObjectInputStream ois = new ObjectInputStream(bais);

        T obj = (T)ois.readObject();

        return obj;
    }

    //extends Serializable
    public static <T> String collectionSerialize(List<T> objs) throws Exception {
        if (objs == null || objs.size() == 0) {
            return null;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(2 * 1024);
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeInt(objs.size());
        Iterator<T> it = objs.iterator();
        while(it.hasNext()) {
            oos.writeObject((T)(it.next()));
        }
        oos.close();

        return Base64.encodeBase64String(baos.toByteArray());
    }

    // extends Serializable
    public static <T> List<T> collectionDeSerialize(String str) throws Exception {
        byte[] bts = Base64.decodeBase64(str);

        List<T> outdocs = new ArrayList<T>();

        ByteArrayInputStream bais = new ByteArrayInputStream(bts);
        ObjectInputStream ois = new ObjectInputStream(bais);
        int size = ois.readInt();
        logger.info(" size = " + size);

        for (int i=0; i<size; i++) {
            T in = (T)ois.readObject();
            outdocs.add(in);
        }

        return outdocs;
    }

    public static <T extends TBase> String thriftObjSerialize(T obj) throws Exception {
        TMemoryBuffer mb = new TMemoryBuffer(128);
        TProtocol prot = new org.apache.thrift.protocol.TCompactProtocol(mb);
        obj.write(prot);

        byte[] out = mb.getArray();

        return Base64.encodeBase64String(out);
    }

    public static <T extends TBase> T thriftObjDeSerialize(String str, T orgObj) throws Exception {
        byte[] out = Base64.decodeBase64(str);
        TMemoryBuffer mb = new TMemoryBuffer(128);
        TProtocol prot = new org.apache.thrift.protocol.TCompactProtocol(mb);
        mb.write(out);
        orgObj.read(prot);

        return orgObj;
    }

}
