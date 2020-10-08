package cc.yuerblog;

import cc.yuerblog.dag.Collection2Text;
import cc.yuerblog.dag.Seq2RDD;
import cc.yuerblog.dag.Text2Seq;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

// https://spark.apache.org/docs/latest/rdd-programming-guide.html
// yarn提交命令：spark-submit --master yarn --deploy-mode cluster --executor-memory 1G --num-executors 10 ./spark-demo-1.0-SNAPSHOT.jar
public class Main {
    public static void main(String []args) {
        try {
            FileSystem dfs = FileSystem.get(new Configuration());               // hdfs连接
            SparkConf conf = new SparkConf().setAppName("spark-demo");              // 配置
            JavaSparkContext sc = new JavaSparkContext(conf);               // Spark连接

            // JAVA数组转RDD
            Collection2Text collection2Text = new Collection2Text();
            collection2Text.run(dfs, sc);

            // HDFS Text文件转RDD，RDD转sequenceFile文件
            Text2Seq text2Seq = new Text2Seq();
            text2Seq.run(dfs, sc);

            // Hdfs SequenceFile转RDD，简单RDD计算
            Seq2RDD seq2RDD = new Seq2RDD();
            seq2RDD.run(dfs, sc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
