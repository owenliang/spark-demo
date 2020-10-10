package cc.yuerblog.dag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;

public class Text2Seq {
    // 静态内部类
    private static class MyPairFunction implements PairFunction<String, NullWritable, Text> {
        public Tuple2<NullWritable, Text> call(String s) throws Exception {
            return new Tuple2<NullWritable, Text>(NullWritable.get(), new Text(s));
        }
    }

    public void run(FileSystem dfs, JavaSparkContext sc) throws IOException {
        String path = "/Hdfs2RDD";

        // 删除输出目录
        dfs.delete(new Path(path), true);

        // 将Collection2RDD的text输出作为输入
        JavaRDD<String> distData = sc.textFile("/Collection2RDD");

        // 处理成K-V格式的RDD
        JavaPairRDD pairRDD = distData.mapToPair(new MyPairFunction()); // 该transformer对象会被序列化传到executor机器上执行

        // 按sequenceFile保存到hdfs的对应目录下，采用Gzip压缩
        //
        // 需要准备一个hadoop的configuration来定义输出格式的配置
        Configuration conf = new Configuration();
        conf.setBoolean(FileOutputFormat.COMPRESS, true);   // 开启压缩
        conf.setClass(FileOutputFormat.COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);    // 压缩类型GZIP，父类是CompressionCodec
        conf.set(FileOutputFormat.COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.toString());    // 设置块级压缩
        pairRDD.saveAsNewAPIHadoopFile(path, NullWritable.class, Text.class, SequenceFileOutputFormat.class, conf); // sequenceFile的K是NullWritable，V是text
    }
}
