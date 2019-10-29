package HBaseAPI.Coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.InternalScan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MyFirstCoprocessorEndpoint extends MyFirstCoprocessor.TestService implements Coprocessor, CoprocessorService {

    private static Log log = LogFactory.getLog(MyFirstCoprocessorEndpoint.class);


    private RegionCoprocessorEnvironment env;

    @Override
    public void getAvg(RpcController controller, MyFirstCoprocessor.MyFirstRequest request, RpcCallback<MyFirstCoprocessor.MyFirstResponse> done) {
        MyFirstCoprocessor.MyFirstResponse response = null;


        long counter = 0L;
        List<Cell> results = new ArrayList<>();
        InternalScanner internalScan = null;
        try {
            log.info("Start avg endpoint.........................");
            Scan scan = null;
            ClientProtos.Scan cScan = request.getScan();
            if(cScan!=null){
                scan = ProtobufUtil.toScan(request.getScan());
                byte[] startRow = scan.getStartRow();
                byte[] stopRow = scan.getStopRow();
                if (startRow != null && stopRow != null)
                    log.info("StartRow = " + Bytes.toStringBinary(startRow) +
                            ", StopRow = " + Bytes.toStringBinary(stopRow));
            }else {
                scan = new Scan();
            }
                byte[] cf = Bytes.toBytes(request.getFamily());
                scan.addFamily(cf);
                //传入列的方式   sales,sales
                String colums = request.getColumns();
                log.info("Input colums: " + colums);
                Map<String, Long> columnMaps = new HashedMap();
                for (String column : colums.split(",")) {
                    columnMaps.put(column, 0L);
                    scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes(column));
                }
                internalScan =  this.env.getRegion().getScanner(scan);
                boolean hasMoreRows = false;
                do {
                    hasMoreRows = internalScan.next(results);

                    if (results.size() > 0) {
                        counter++;
                    }
                    /*
                    for (Cell cell : results) {
                        String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        Long temp = Long.parseLong(value);
                        columnMaps.put(column, columnMaps.get(column) + temp);
                    }

                     */

                    results.clear();
                } while (hasMoreRows);
                StringBuffer values = new StringBuffer();
                /*
                for (String key : columnMaps.keySet()) {
                    Long value = columnMaps.get(key);
                    values.append(key).append(":").append(value).append(",");
                }
                log.info("avg server result: " + values);

                 */
                values.append("hhhh");
                response = MyFirstCoprocessor.MyFirstResponse.newBuilder()
                        .setCount(counter)
                        .setValues(values.toString())
                        .build();




        }catch (Exception e){

            response = MyFirstCoprocessor.MyFirstResponse.newBuilder()
                    .setCount(0l)
                    .setValues("dsasadads")
                    .build();

        }finally {
            if (internalScan != null) {
                try {
                    internalScan.close();
                } catch (IOException ignored) {
                }
            }
        }


        done.run(response);


    }

    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {

        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }

    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {

    }

    @Override
    public Service getService() {
        return this;
    }



}
