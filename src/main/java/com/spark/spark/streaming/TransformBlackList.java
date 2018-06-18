package com.spark.spark.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;

//实时黑名单过滤
public class TransformBlackList {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(15));

        //过滤掉黑名单中的用户点击的广告

        //模拟黑名单RDD
        ArrayList<Tuple2<String, Boolean>> blackList = new ArrayList<Tuple2<String, Boolean>>();
        blackList.add(new Tuple2<String, Boolean>("bob",true));
        final JavaPairRDD<String, Boolean> blackListRDD = jssc.sc().parallelizePairs(blackList);

        //日志简化为date username的形式
        JavaReceiverInputDStream<String> adsCloickLogDStream = jssc.socketTextStream("node1", 9999);
        //将输入转换为(username,date username)的形式，便于后面对每个batch RDD与定义好的黑名单RDD进行join操作
        JavaDStream<String> res = adsCloickLogDStream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[1], s);
            }
        }).transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> s) throws Exception {
                //使用左外链接，因为多数用户是不在黑名单中的
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = s.leftOuterJoin(blackListRDD);
                //执行过滤算子
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> t) throws Exception {
                        if (t._2._2().isPresent() && t._2._2.get()) {
                            return false;
                        }
                        return true;
                    }
                });

                //剩下未被过滤的
                return filteredRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> t) throws Exception {
                        return t._2._1;
                    }
                });
            }
        });
        res.print();
        //就可以执行transform操作，将每个batch的rdd与黑名单RDD进行join filter map等操作

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
