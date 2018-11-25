package com.example.spark.sql.util;

import org.apache.log4j.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by yilong on 2018/6/10.
 */
public class ThriftSerde {
    private static Logger logger = Logger.getLogger(ThriftSerde.class);

    public static <T extends org.apache.thrift.TBase> byte[] serialize(T obj) {
        TMemoryBuffer mb1 = new TMemoryBuffer(32);
        TProtocol prot = new org.apache.thrift.protocol.TBinaryProtocol(mb1);

        try {
            obj.write(prot);
        } catch (TException e) {
            logger.error(e.getMessage(), e);
            throw new IllegalArgumentException(" object is not TBase type ");
        }

        return mb1.getArray();
    }

    @SuppressWarnings("unchecked") //extends org.apache.thrift.TBase
    public static <T> T deSerialize(Class<T> cls, byte[] data) {
        T obj = null;
        try {
            obj = (T) Class.forName(cls.getName()).newInstance();
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        } catch (InstantiationException e) {
            logger.error(e.getMessage(), e);
        }

        if (obj == null) {
            throw new RuntimeException(cls.getName() + " : Class.forName().newInstance() failed!");
        }

        if (!(obj instanceof org.apache.thrift.TBase)) {
            throw new RuntimeException(cls.getName() + " : not a org.apache.thrift.TBase type!");
        }

        TMemoryBuffer tmb = new TMemoryBuffer(32);
        try {
            tmb.write(data);
            TProtocol tp = new org.apache.thrift.protocol.TBinaryProtocol(tmb);
            ((org.apache.thrift.TBase)obj).read(tp);
        } catch (TTransportException e) {
            logger.error(e.getMessage(), e);
        } catch (TException e) {
            logger.error(e.getMessage(), e);
        }

        return obj;
    }
}
