package com.spark.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

//基于滑动窗口的热点搜索词实时统计
public class WindowHotWord {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        //格式  username searchword
        JavaReceiverInputDStream<String> searchLog = jssc.socketTextStream("node1", 9999);
        //将搜索日志的搜索词找出来
        searchLog.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.split(" ")[1];
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                //映射为(word,1)
                return new Tuple2<String, Integer>(s,1);
            }
        }).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            //针对(word,1)的dstream，执行滑动窗口操作
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
            //参数二为窗口长度，参数三为滑动间隔，也就是说每隔10秒将最近60秒钟的数据作为一个窗口，进行内部的RDD的聚合操作，然统一对RDD做后续处理
            //只有等到我们的滑动间隔到了之后，会将之前的60秒钟的RDD，因为一个batch间隔是5秒，所以之前60秒钟，就有12个RDD，给聚合起来，
            // 然后统一执行reduceByKey操作，所以这里的reducebykeyandwindow，是针对每个窗口执行计算的，而不是针对某一个dstream中的RDD计算的
        },Durations.seconds(60),Durations.seconds(10))//执行transform操作，因为一个窗口就是一个60秒钟的数据会变成一个RDD，
                // 然后对这一个RDD根据每个搜索词出现的频率进行排序，获取排名前3的热点搜索词
         .transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
             @Override
             public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> t) throws Exception {
                 //执行搜索词和词频的反转
                 JavaPairRDD<String, Integer> ret = t.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                     @Override
                     public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                         return new Tuple2<Integer, String>(t._2, t._1);
                     }
                 }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                     @Override
                     public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                         return new Tuple2<String, Integer>(t._2, t._1);
                     }
                 });

                 for(Tuple2<String, Integer> take:ret.take(3)){
                     System.out.println("SearchWord: " + take._1 + " Count: " + take._2);
                 }

                return ret;
             }
         }).print();//只是为了触发job的执行，所以必须要求output操作

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
