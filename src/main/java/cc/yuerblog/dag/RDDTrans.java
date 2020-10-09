package cc.yuerblog.dag;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class RDDTrans {
    // RDD单分区聚合函数
    private static class SeqFunction implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer a, Integer b) throws Exception {
            return a+b;
        }
    }
    // RDD多分区合并函数
    private static class CombFunction implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer a, Integer b) throws Exception {
            return a+b;
        }
    }

    public void run(FileSystem dfs, JavaSparkContext sc) throws IOException {
        List<Tuple2<String, String>> words = new ArrayList<Tuple2<String, String>>();
        words.add(new Tuple2<String, String>("hello", "你好"));
        words.add(new Tuple2<String, String>("bye", "再见"));
        words.add(new Tuple2<String, String>("world", "世界"));

        // 词典RDD，用于JOIN
        JavaPairRDD dictRDD = sc.parallelizePairs(words);

        // 把字典cache起来，后续用来join更快
        dictRDD.cache();

        // 随机单词RDD： <单词, 1>
        List<Tuple2<String, Integer>> l = new ArrayList<Tuple2<String, Integer>>();
        for (int i = 0; i < 1000; i++) {
            int index = new Random().nextInt(words.size());
            l.add(new Tuple2<String, Integer>(words.get(index)._1(), 1));
        }
        JavaPairRDD wordsRDD = sc.parallelizePairs(l);

        // 单词次数统计，采用比较底层也比较灵活的一种API
        JavaPairRDD aggRDD = wordsRDD.aggregateByKey(Integer.valueOf(0), new SeqFunction(), new CombFunction());

        // 把统计好的单词和词典做left join
        // JOIN返回值的K是2个RDD的K，而V则是2个RDD的V组成的Tuple，因为是左连接所以右侧RDD的V可能没有，因此是Optional的
        JavaPairRDD<String, Tuple2<Integer, Optional<String>>> joinedRDD = aggRDD.leftOuterJoin(dictRDD);

        // 打印JOIN好的数据
        System.out.println(joinedRDD.collect());
    }
}
