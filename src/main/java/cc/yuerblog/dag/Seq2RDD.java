package cc.yuerblog.dag;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.IOException;

public class Seq2RDD {
    private static class ExtractValueFromPair implements Function<Tuple2<NullWritable, Text>, String> {
        public String call(Tuple2<NullWritable, Text> t) throws Exception {
            return t._2().toString();
        }
    }
    public void run(FileSystem dfs, JavaSparkContext sc) throws IOException {
        // 加载sequenceFile，得到K-V RDD
        JavaPairRDD rdd = sc.newAPIHadoopFile("/Hdfs2RDD", SequenceFileInputFormat.class, NullWritable.class, Text.class, sc.hadoopConfiguration());

        // 取出所有的V
        JavaRDD rows = rdd.map(new ExtractValueFromPair());

        // 可以令--deploy-mode client，这样spark AM将跑在本地，collect的数据将直接打印在本地终端，该方式用于调试
        System.out.println(rows.collect());
    }
}
