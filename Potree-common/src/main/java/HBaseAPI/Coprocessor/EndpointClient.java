package HBaseAPI.Coprocessor;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class EndpointClient {

    private static Logger log  = Logger.getLogger("EndpointClientLog");

    public static long execFastEndpointCoprocessor(Table table, final Scan scan, final String family, final String cloumes){

        long start_t = System.currentTimeMillis();
        //定义总的 rowCount 变量
        final AtomicLong totalRowCount = new AtomicLong();
        final Map<String, AtomicLong> sumMap = new HashMap<>();

        try {
            Batch.Callback<MyFirstCoprocessor.MyFirstResponse> callback = new Batch.Callback<MyFirstCoprocessor.MyFirstResponse>() {
                @Override
                public void update(byte[] bytes, byte[] bytes1, MyFirstCoprocessor.MyFirstResponse myFirstResponse) {

                    //更新Count值
                    totalRowCount.getAndAdd(myFirstResponse.getCount());
                    String values = myFirstResponse.getValues();
                    System.out.println("values:"+values);
                    /*
                    for(String kv : values.split(",")) {
                        String[] kvs = kv.split(":");
                        String key = kvs[0];
                        Long value = Long.parseLong(kvs[1]);
                        if (!sumMap.containsKey(key)) {
                            final AtomicLong sum = new AtomicLong();
                            sum.getAndAdd(value);
                            sumMap.put(key, sum);
                        } else {
                            sumMap.get(key).getAndAdd(value);
                        }
                    } */
                }


            };

            final ClientProtos.Scan cScan = ProtobufUtil.toScan(scan);

            table.coprocessorService(MyFirstCoprocessor.TestService.class, scan.getStartRow(), scan.getStopRow(),
                    new Batch.Call<MyFirstCoprocessor.TestService, MyFirstCoprocessor.MyFirstResponse>() {
                        @Override
                        public MyFirstCoprocessor.MyFirstResponse call(MyFirstCoprocessor.TestService testService) throws IOException {

                            MyFirstCoprocessor.MyFirstRequest myFirstRequest = MyFirstCoprocessor.MyFirstRequest.newBuilder()
                                    .setScan(cScan)
                                    .setFamily(family)
                                    .setColumns(cloumes)
                                    .build();
                            BlockingRpcCallback<MyFirstCoprocessor.MyFirstResponse> rpcCallback = new BlockingRpcCallback<>();
                            testService.getAvg(null,myFirstRequest,rpcCallback);

                            MyFirstCoprocessor.MyFirstResponse response = rpcCallback.get();
                            if(response==null)
                                System.out.println("response is null");
                            return  response;

                        }
                    },callback);

        }catch (Exception e){
            log.severe("协处理器客户端出错");
            log.severe(e.toString());
            e.printStackTrace();

        }catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.out.println("耗时：" + (System.currentTimeMillis() - start_t));
        System.out.println("totalRowCount：" + totalRowCount.longValue());
        /*
        for (String key : sumMap.keySet()) {
            Double value = sumMap.get(key).doubleValue();
            System.out.println(key + " avg = " + value / totalRowCount.longValue());
        }
        */
        return totalRowCount.longValue();


    }

}
