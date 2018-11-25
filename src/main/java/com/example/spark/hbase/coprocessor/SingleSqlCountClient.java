package com.example.spark.hbase.coprocessor;

import com.example.spark.hbase.filter.SingleAvroFilter;
import com.example.spark.hbase.generated.SingleSqlCount;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

/**
 * Created by yilong on 2018/9/13.
 */
public class SingleSqlCountClient {
    private static Logger logger = Logger.getLogger(SingleAvroFilter.class);
    private String tableName;
    private String family;
    private String column;
    private String avroFilterStr;

    public SingleSqlCountClient(String tableName, String family, String column, SingleAvroFilter avroFilter) {
        this.tableName = tableName;
        this.family = column;
        this.column = column;
        try {
            this.avroFilterStr = Base64.getEncoder().encodeToString(avroFilter.toByteArray());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }
    public SingleSqlCountStatis doCount() throws IOException {
        SingleSqlCountStatis statis = new SingleSqlCountStatis();
        statis.count = 0L;
        statis.byteSize = 0L;

        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        final SingleSqlCount.SqlCountRequest request =
                SingleSqlCount
                        .SqlCountRequest
                        .newBuilder()
                        .setFamily(family)
                        .setColumn(column)
                        .setAvroFilter(avroFilterStr)
                        .build();
        try {
            Map<byte[], SingleSqlCountStatis> results = table.coprocessorService(
                    SingleSqlCount.SingleSqlCountService.class,
                    null,
                    null,
                    new Batch.Call<SingleSqlCount.SingleSqlCountService, SingleSqlCountStatis>() {
                        @Override
                        public SingleSqlCountStatis call(SingleSqlCount.SingleSqlCountService countService) throws IOException {
                            BlockingRpcCallback<SingleSqlCount.SqlCountResponse> rpcCallback = new BlockingRpcCallback();
                            countService.getCount(null, request, rpcCallback);
                            SingleSqlCount.SqlCountResponse response = rpcCallback.get();
                            SingleSqlCountStatis statis = new SingleSqlCountStatis();
                            statis.count = response.hasCount() ? response.getCount() : 0L;
                            statis.byteSize = response.hasByteSize() ? response.getByteSize() : 0L;
                            return statis;
                        }
                    });
            for (SingleSqlCountStatis tmp : results.values()) {
                statis.count += tmp.count;
                statis.byteSize += tmp.byteSize;
                logger.info("SingleSqlCount.SqlCountResponse.Statis : " + statis.count + ", " + statis.byteSize);
            }
        } catch (ServiceException e) {
            logger.error(e.getMessage(), e);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        return statis;
    }
}
