package cc.yuerblog;

import cc.yuerblog.dag.Collection2RDD;
import cc.yuerblog.dag.Hdfs2RDD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

// https://spark.apache.org/docs/latest/rdd-programming-guide.html
// 启动命令：
// spark-submit --master yarn --deploy-mode cluster --executor-memory 1G --num-executors 10 ./spark-demo-1.0-SNAPSHOT.jar
public class Main {
    public static void main(String []args) {
        try {
            // hdfs连接
            FileSystem dfs = FileSystem.get(new Configuration());
            // 配置
            SparkConf conf = new SparkConf().setAppName("spark-demo");
            // Spark连接
            JavaSparkContext sc = new JavaSparkContext(conf);

            // JAVA数组转RDD
            Collection2RDD collection2RDD = new Collection2RDD();
            collection2RDD.run(dfs, sc);

            // HDFS Text文件转RDD，RDD转sequenceFile文件
            Hdfs2RDD hdfs2RDD = new Hdfs2RDD();
            hdfs2RDD.run(dfs, sc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
