package com.example.spark.hbase.coprocessor;

import com.example.spark.hbase.filter.SingleAvroFilter;
import com.example.spark.hbase.generated.SingleSqlCount;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Created by yilong on 2018/9/13.
 */
public class SingleSqlCountEndpoint
        extends SingleSqlCount.SingleSqlCountService
        implements Coprocessor, CoprocessorService {

    private RegionCoprocessorEnvironment env;

    @Override
    public void getCount(RpcController controller,
                         SingleSqlCount.SqlCountRequest request,
                         RpcCallback<SingleSqlCount.SqlCountResponse> done) {
        String family = request.getFamily();
        String column = request.getColumn();
        String avroFilterStr = request.getAvroFilter();
        byte[] avroFilterBytes = Base64.getDecoder().decode(avroFilterStr);
        SingleAvroFilter filter = SingleAvroFilter.parseFrom(avroFilterBytes);

        Scan s = new Scan();
        s.setFilter(filter);

        s.addFamily(Bytes.toBytes(request.getFamily()));
        s.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes(request.getColumn()));

        SingleSqlCount.SqlCountResponse response = null;

        InternalScanner scanner = null;
        try {
            scanner = env.getRegion().getScanner(s);
            List<Cell> results = new ArrayList();
            boolean hasMore = false;
            long sum = 0L;
            long byteSize = 0L;
            do {
                hasMore = scanner.next(results);
                sum += results.size();
                byteSize += results.toArray().length;
                results.clear();
            } while (hasMore);

            response = SingleSqlCount.SqlCountResponse.newBuilder().setCount(sum).setByteSize(byteSize).build();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {
                    ignored.printStackTrace();
                }
            }
        }
        done.run(response);
    }

    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        if (coprocessorEnvironment instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)coprocessorEnvironment;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        //TODO: do nothing
    }

    @Override
    public Service getService() {
        return this;
    }
}
