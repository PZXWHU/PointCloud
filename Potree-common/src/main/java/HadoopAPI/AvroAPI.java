package HadoopAPI;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;

public class AvroAPI {



    public static void main(String[] args){



        //加载资源
        //AvroAPI.class.getClassLoader().getResource("");//file:/C:/Users/PZX/Desktop/Potree/Potree-common/target/classes/  类路径
        //AvroAPI.class.getClassLoader().getResource("/"); //null

        //AvroAPI.class.getResource(""); //file:/C:/Users/PZX/Desktop/Potree/Potree-common/target/classes/HadoopAPI/   带有包名的类路径
        //AvroAPI.class.getResource("/");  //file:/C:/Users/PZX/Desktop/Potree/Potree-common/target/classes/     类路径

        //System.getProperty("user.dir"); //C:\Users\PZX\Desktop\Potree  工程目录
        //new File("").getAbsolutePath(); //C:\Users\PZX\Desktop\Potree  工程目录
        //new File("").getCanonicalPath()  //C:\Users\PZX\Desktop\Potree  工程目录


        try {

            //使用模式文件！！！！！！

            Schema.Parser parser = new Schema.Parser();//模式解析器
            Schema schema = parser.parse(AvroAPI.class.getClassLoader().getResourceAsStream("avro/StringPair.avsc"));//利用模式文件解析获取avro模式

            GenericRecord datum = new GenericData.Record(schema);//利用模式创建record实例
            datum.put("left","L");
            datum.put("right","R");

            //将记录序列化到流中
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);//利用模式创建GenericDatumWriter，其将对象翻译成Encoder对象可以理解的类型，再由后者写入流
            Encoder encoder = EncoderFactory.get().binaryEncoder(out,null);
            writer.write(datum,encoder);
            encoder.flush();
            out.close();

            //从字节缓冲区中读取record对象
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),null);
            GenericRecord result = reader.read(null,decoder);
            System.out.println(result);




            //使用模式文件生成的类！！！！

            StringPair datum1 = new StringPair();
            datum1.setLeft("LL");
            datum1.setRight("RR");

            ByteArrayOutputStream out1 = new ByteArrayOutputStream();
            DatumWriter<StringPair> writer1 = new SpecificDatumWriter<>(StringPair.class);
            Encoder encoder1 = EncoderFactory.get().binaryEncoder(out1,null);
            writer1.write(datum1,encoder1);
            encoder1.flush();
            out1.close();

            DatumReader<StringPair> reader1 = new SpecificDatumReader<>(StringPair.class);
            Decoder decoder1 = DecoderFactory.get().binaryDecoder(out1.toByteArray(),null);
            StringPair result1 = reader1.read(null,decoder1);
            System.out.println(result1);



            //数据文件，将序列化对象写到文件中

            File file = new File("data.avro");
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
            dataFileWriter.create(schema,file);//也可以把file替换为outputsteam，从而将数据写到流中.例如FileSystem.create()在HDFS中创建文件并返回输入流，即可把文件写入HDFS
            dataFileWriter.append(datum);
            dataFileWriter.close();

            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file,reader);//可以使用avro的FsInput对象来指定使用Hadoop Path对象作为输入对象，从而从HDFS中读取数据
            if(dataFileReader.hasNext()){
                GenericRecord result3 = dataFileReader.next();
                System.out.println(result3);
            }


        }catch (Exception e){
            System.out.println("AvroAPI失败");
            e.printStackTrace();
        }







    }

}

