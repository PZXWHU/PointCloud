package com.pzx.dataSplit;

import com.pzx.BulkLoad;
import com.pzx.geometry.Point3D;
import com.pzx.pointCloud.PointCloud;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataSplitter {

    protected String inputDirPath;

    protected String outputDirPath;

    protected SparkSession sparkSession;

    protected PointCloud pointCloud;

    protected static final String CLOUD_JS_FILENAME = "cloud.js";

    protected static final String HRC_FILENAME = "r.hrc";

    private static Logger logger = LoggerFactory.getLogger(DataSplitter.class);

    public DataSplitter(String inputDirPath, String outputDirPath) {
        this.inputDirPath = inputDirPath;
        this.outputDirPath = outputDirPath;
    }

    public final void dataSplit(){
        long time = System.currentTimeMillis();
        sparkSessionInit();
        loadData();
        createCloudJS();
        splitData();
        createHrc();
        bulkLoad();
        sparkSessionClose();
        logger.info("-----------------------------------此次点云分片任务全部耗时为："+(System.currentTimeMillis()-time));
    }

    abstract protected void sparkSessionInit();

    abstract protected void loadData();

    abstract protected void createCloudJS();

    abstract protected void splitData();

    abstract protected void createHrc();

    /**
     * 默认空实现，需要直接进行bulkload的任务，重写此方法即可
     */
    protected void bulkLoad(){

    }

    private void sparkSessionClose(){
        sparkSession.stop();
        sparkSession.close();
    }


}
