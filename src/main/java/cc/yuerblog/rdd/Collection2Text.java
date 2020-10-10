package cc.yuerblog.dag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

// 本地数据 转 RDD
public class Collection2Text {
    public void run(FileSystem dfs, JavaSparkContext sc) throws IOException {
        String path = "/Collection2RDD";

        // 删除输出目录
        dfs.delete(new Path(path), true);

        // JAVA集合转成多分片RDD
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        // 保存到hdfs的对应目录下
        distData.saveAsTextFile(path);
    }
}
