package com.pzx.dataSplit;

import com.alibaba.fastjson.JSONObject;
import com.pzx.IOUtils;
import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import com.pzx.pointCloud.PointAttribute;
import com.pzx.pointCloud.PointCloud;
import org.apache.commons.io.Charsets;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

/**
 * 针对某特定文本格式的读取
 */
public abstract class TxtSplitter extends DataSplitter {

    protected Dataset<Row> rowDataSet;

    public TxtSplitter(String inputDirPath, String outputDirPath) {
        super(inputDirPath, outputDirPath);
    }

    @Override
    protected void loadData() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("x", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("y", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("z", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("intensity", DataTypes.IntegerType, false));//此处使用ByteType并没有太大关系，其不会造成同行的所有列变为ull，因为谓词下推功能，此列数据实际上并没有被解析。
        fields.add(DataTypes.createStructField("r", DataTypes.IntegerType, false));//此处使用IntegerType是因为如果使用ByteType其只能解析范围在[-128,127]内的整数，对于大于127的整数解析为null，并且会造成同行所有的列都被解析为null；
        fields.add(DataTypes.createStructField("g", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("b", DataTypes.IntegerType, false));
        StructType scheme = DataTypes.createStructType(fields);

        //读取数据
        rowDataSet = sparkSession.read()
                .format("csv")
                .option("sep"," ")
                .schema(scheme) //.option("inferSchema","true") 模式推理会导致源数据被加载两遍
                .load(inputDirPath)
                .selectExpr("x","y","z","r","g","b");
        rowDataSet.persist(StorageLevel.MEMORY_AND_DISK_SER());

    }

    @Override
    protected void createCloudJS() {
        Dataset<Row> cloudJSDataSet  = rowDataSet.select(min("x"),min("y"),min("z"),
                max("x"),max("y"),max("z"),count(col("x")));

        Row cloudJSRow = cloudJSDataSet.collectAsList().get(0);
        long points = cloudJSRow.getLong(6);
        Cuboid tightBoundingBox =  new Cuboid(cloudJSRow.getDouble(0),cloudJSRow.getDouble(1),cloudJSRow.getDouble(2),
                cloudJSRow.getDouble(3),cloudJSRow.getDouble(4),cloudJSRow.getDouble(5));

        double[] scales = new double[]{0.001,0.001,0.001};

        List<PointAttribute> pointAttributes = Arrays.asList(PointAttribute.POSITION_XYZ, PointAttribute.RGB) ;

        pointCloud = new PointCloud(points,tightBoundingBox,pointAttributes,scales);
        JSONObject cloudJS = pointCloud.buildCloudJS();
        IOUtils.writerDataToFile(outputDirPath + File.separator + CLOUD_JS_FILENAME,cloudJS.toJSONString().getBytes(Charsets.UTF_8),false);
    }

}
