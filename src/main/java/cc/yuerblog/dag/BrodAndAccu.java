package cc.yuerblog.dag;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.io.IOException;
import java.util.Arrays;

public class BrodAndAccu {
    private static class FilterFunc implements Function<Integer, Boolean> {
        private Broadcast<Long> v;
        private LongAccumulator accum;
        public FilterFunc(Broadcast<Long> v, LongAccumulator accum) {
            this.v = v;
            this.accum = accum;
        }
        public Boolean call(Integer integer) throws Exception {
            if (Long.valueOf(integer) >= v.value()) {// 保留大于v的数字
                accum.add(1);
                return true;
            }
            return false ;
        }
    }

    public void run(FileSystem dfs, JavaSparkContext sc) throws IOException {
        // 创建广播变量
        Broadcast<Long> v = sc.broadcast(Long.valueOf(3));
        // 创建计数器，用于统计大于v的数字个数
        LongAccumulator accum = sc.sc().longAccumulator("validCount");

        // 生成RDD
        JavaRDD rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));

        // 过滤RDD
        JavaRDD filtered = rdd.filter(new FilterFunc(v, accum));

        // 在driver端收集结果
        System.out.println(filtered.collect());
        System.out.println(accum.value());
    }
}
